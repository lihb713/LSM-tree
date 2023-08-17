# builder



# 为什么需要builder

builder用于将一个sstable所有数据进行序列化，形成一个字节序列。之所以将sstable的构建工作抽离出来。是因为我们可能需要将不同格式的数据序列化为sstable。比如将LSM tree中的memtable固化到磁盘时需要这序列化。将LSM tree中底层sstable进行merge操作合并时仍然需要序列化。因此我们将这个功能抽离出来。


# builder序列化对象

在解读builder实现前，我们先看看需要序列化过程中我们需要处理的数据。这些数据结构跟我们要序列化的sstable结构不等价，但是是相关的。其中一些变量在我们计算sstable中要保存的数据时会被用到：

首先是buildData，其和sstable基本是等价的，其中存储了sstable序列化后的信息，如blockList就是序列化后的block，index就是序列化后的table index。只不过还没有使用mmap写入磁盘。我们的builder最终目的就是为了构建一个序列化好的sstable，即buildData。

```go
type buildData struct {
	blockList []*block //block列表
	index     []byte  //table index
	checksum  []byte
	size      int
}
```

下面两个类型和builder密切相关，它们也是builder的一部分。

block类型是构建block时需要的数据结构，这里block结构体和并不是序列化后的block，而是一个block的相关信息，包括构造过程中的中间变量以及table index中存储的关于block的信息。可以认为block类型和后面的tableBuilder在逻辑上用途一致，都是为了构建sstable设置的类型。只不过block类型用于构建block，tableBuilder用于构建sstable。

header则对应每一条entry数据的头部，记录了entry的key与base key相同部分长度和不同部分长度。

```go
type block struct {
	offset            int //当前block的offset 首地址
	checksum          []byte
	entriesIndexStart int  //记录entryOffsets起始位置，方便获取data
	chkLen            int
	data              []byte  //entry数据
	baseKey           []byte   //base key
	entryOffsets      []uint32 //entry的offset列表
	end               int  //当前block data字段长度（最后一个字节偏移量），用于构造table index中得block offsets
	estimateSz        int64  //估计大小，判断当前block是否到最大长度
}

type header struct {
	overlap uint16 // 与base key重叠部分
	diff    uint16 // 与base key不同部分字节长度
}
```

下面是编解码header的两个方法：

```go
// 解码header指针并返回字节数组
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize]) //将header指针转为字节数组指针，并拷贝
}

// header编码为字节数组
func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}
```

# builder


## 数据结构


```go
// table构造器
type tableBuilder struct {
	sstSize       int64    //sstable大小
	curBlock      *block   //当前block指针
	opt           *Options //LSM的选项
	blockList     []*block //block列表
	keyCount      uint32   //key数量
	keyHashes     []uint32 //key哈希列表，用于构建bloom filter
	maxVersion    uint64   //最大版本（时间戳）
	baseKey       []byte   //base key
	staleDataSize int
	estimateSz    int64
}
```

```go
// 使用指定size创建一个tableBuilder
func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}

// 创建一个tableBuilder
func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}
```

## 向tableBuilder中添加entry

```go
// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, true)
}

// AddKey 增加非过期entry
func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}
```

```go
// 向curBlock中添加一条entry
func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
    // 提取KV
	key := e.Key
	val := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}

    /* 下面是sstable相关数据构建、主要是table index */
	// 检查是否需要分配一个新的 block
	if tb.tryFinishBlock(e) {
		if isStale {
			// 这个key将被加入table index并且它过期了
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock() //将block附加信息写入block，形成完整的block

		//创建一个新block并开始写入
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO 加密block后块的大小会增加，需要预留一些填充位置
		}
	}

	//key中末尾8字节包含了时间戳（我们存储了同一key不同版本），分别解析真正的key和时间戳
    //hashkey加入keyHashes，用于之后构建bloom filter
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))

    // 更新maxVersion
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

    /* 下面是block相关数据构建 */

    // 计算header
	var diffKey []byte
	//当前没有base key，就将当前key作为base key和diff key。否则计算diff key
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}

	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

    //记录当前entry的偏移量
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

    //向curBlock的data中追加entry数据
	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}
```

我们的sst文件中key序列同时存储了数据真正的key以及时间戳，之所以将时间戳和key值拼接到一起是因为LSM-tree总是以追加的方式加入数据，因此可能存在同一个key值的不同版本的数据，这些相同key值得数据无疑是冗余的，我们最后应该只保留最新版本。因此将key和时间戳拼接到一起方便我们舍弃旧版本得key值，具体得删除操作将在compact中实现。这里重点注意到entry中的key以及sstable中保存的key都是包含时间戳的。

在向builder中添加entry的过程中，用到allocate和append函数，这两个函数用来从当前block，即curBlock的data字段获取空间以及向data中追加数据。每次调用allocate方法，我们都会更新curBlock的end字段，其指向block中data字段的最后一个字节的下一字节位置。

```go
// 向curBlock.data中添加数据
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

// 从curBlock.data中分配内存空间
func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// 需要reallocate
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}
```

向builder中添加数据，多数是与block的data和entryOffset字段相关的操作。还有一些是和table index相关的，如keyHashes加入新的键值对keyHash用于最终生成bloom filter以及更新maxVersion和stableDataSize。

但除此之外，当我们当前block已经到达最大block大小时，我们还需要封装一个完整的block并且更新table index其他block相关字段。tryFinishBlock方法用于判断当前block是否应该封装、finishBlock为当前block加入其余信息完成封装。

```go
// 判断curBlock是否已经满
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))

    /* 下面计算curBlock再加入一条新的entry后的估计大小，注意是估计值 */
    //entriesOffsetsSize是除了data字段以外的block数据大小
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // entry offset length
		8 + // checksum
		1) // checksum length
    //estimateSz = end + 新entry估计大小 + entriesOffsetsSize
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// 为block添加尾部其他信息，完成封装
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}

    /* 下面完成curBlock的封装 */
	// 追加entryOffset和它的长度
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

    // 追加checksum和它的长度
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

    /* 下面更新sstable中table index相关数据 */
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: 预估整理builder写入磁盘后，sst文件的大小
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))

    tb.estimateSz += tb.curBlock.estimateSz
	tb.curBlock = nil // 表示当前block 已经被序列化到内存
	return
}
```

下面是上面代码中用到的一些辅助函数。calculateChecksum用于生成字节序列的校验和、keyDiff用于计算newKey和base key的不同部分。

```go
// 计算checksum
func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

// 计算newKey和当前block的base key不同部分
func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}
```

## 封装为buildData

flush函数将使用tableBuilder构建出buildData，使用buildData封装为实际sstable格式并用它初始化一个SSTable对象。

```go
// 将tableBuilder构建为table
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done() //封装好一个buildData
	t = &table{lm: lm, fid: utils.FID(tableName)}

	// 注意这里如果builder没有正在构建sstable，那么bd将是一个空的buildData。
	// 这里会打开一个已有的sst文件
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})

	//如果bd不为空，下面是有效的。将buildData数据写入buf并拷贝进sstable。
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

// 将buildData中数据拷贝到目标字节序列dst中
func (bd *buildData) Copy(dst []byte) int {
	var written int
	// 写入block数据
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}

	//写入table index及长度
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	//写入checksum及长度
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}
```

其中tableBuilder的done方法用于从builder中构建buildData并返回。其中构建过程实际上就是按照sstable的组成，将对应值从builder中提取并填入buildData中。这里不再赘述。

```go
// 将tableBuilder中数据封装到buildData中。buildData就对应一个sst文件
func (tb *tableBuilder) done() buildData {
	tb.finishBlock() //封装最后一个block
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	//从builder中构建buildData
	bd := buildData{
		blockList: tb.blockList,
	}

	//构建bloom filter
	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	// 构建table index
	index, dataSize := tb.buildIndex(f)
	bd.index = index
	//计算checksum
	checksum := tb.calculateChecksum(index)
	bd.checksum = checksum

	//buildData总大小
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

// 构建table index
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {

	/* 下面构造table index并序列化 */
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)

	data, err := tableIndex.Marshal() //table index序列化
	utils.Panic(err)

	//datasize计算所有block的总大小（疑惑，计算block大小为什么放这里）
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	return data, dataSize
}

// 向table index中写入offsets
func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	//遍历blockList将每个block的offset、base key、length信息写入table index
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

// 返回一个block对应的offset
func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}
```


# blockIterator

和skiplist一样，我们要为sstable提供一个迭代器方便对entry的遍历访问。而sstable通常只会加载单个block到内存中读取数据。因此这里我们提供了blockIterator类实现这些操作。blockIterator同样符合Iterator的接口。

## 数据结构

```go
// block迭代器
type blockIterator struct {
	data         []byte  //block中data数据部分
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block  //block对象

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}
```


## 迭代器设置

由于是blockIterator，因此迭代器是针对某一block的。这里提供setBlock以初始化迭代器。可以认为是blockIterator构造函数。

```go
// setBlock相当于为block迭代器初始化data和entryOffsets字段，其他字段都是默认初始化
func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}
```

setIdx方法用于将迭代器指向block中指定位置的entry，将entry的数据解析出来。setIdx方法将是迭代器接口方法的核心。

```go
// setIdx将blockIterator指向block中索引为i的entry
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i //idx置为i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil

	/* 下面获取entry的起始偏移startOffset和结束偏移endOffset位置 */
	startOffset := int(itr.entryOffsets[i]) //startOffset为第i条entry起始位置

	// base key为空，初始化base key
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)  //解码data中第一个header
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff] //取第一个entry的diffKey
		// 注意这里解释了为什么builder中每一个block的第一个diffKey被设置为baseKey
	}

	var endOffset int                       //endOffset为第i条entry结束位置
	if itr.idx+1 == len(itr.entryOffsets) { // 如果idx指向block最后一个条目
		endOffset = len(itr.data)
	} else { //如果idx没有指向block最后一个条目
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	/* 下面获取entry字节序列并解析数据 */
	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > itr.prevOverlap { //批注：迷惑操作，重复部分直接从base key取不就行了吗
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	e := &utils.Entry{Key: itr.key}
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	itr.val = val.Value
	e.Value = val.Value
	e.ExpiresAt = val.ExpiresAt
	e.Meta = val.Meta
	itr.it = &Item{e: e}
}
```

## seek方法

```go
// 指向block第一个元素
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

// 指向block最后一个元素
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}

// 使用二分搜索指向block中指定key的元素
func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // 我们应该从哪个位置开始二分搜索

	// 通过二分搜索找到符合itr.key>=key的最小key
	// 注意最终itr.key不一定等于key
	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// 如果idx小于起始索引，返回false
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}
```

## 迭代器接口

```go
// blovkIterator的Error
func (itr *blockIterator) Error() error {
	return itr.err
}

// blovkIterator的Next
func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

// blovkIterator的Valid
func (itr *blockIterator) Valid() bool {
	return itr.err != io.EOF
}

// blovkIterator的Rewind
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}

// blovkIterator的Item
func (itr *blockIterator) Item() utils.Item {
	return itr.it
}

// blovkIterator的Close
func (itr *blockIterator) Close() error {
	return nil
}
```

## 补充：并发控制

builder中不需要并发控制，同一时间只会有一个协程操作builder。






































































































