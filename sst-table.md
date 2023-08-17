# table




# table

前面已经介绍了SSTable对象，用于从磁盘加载sst文件中一些信息（主要是table index）到磁盘方便后续对sst的查询等操作。同时在builder中我们介绍了blockIterator迭代器对象来对某个给定的block字节序列中的entry数据进行遍历。

table则封装了SSTable对象，并提供了tableIterator为我们提供对整个sstable中entry数据进行检索的功能。


## 数据结构

```go
type table struct {
	ss  *file.SSTable //sstable
	lm  *levelManager
	fid uint64
	ref int32 // 引用计数，用于回收。原子的
}
```


## 初始化

openTable函数根据给定的tableName和builder对table中的SSTable对象进行初始化。如果

```go
// 根据传入的tableBuilder和tableName加载sst
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	/* 下面通过builder初始化sstable对象并加载sstable的table index到sstable对象 */
	//获得builder中buildData的大小
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)

	if builder != nil { // 对builder存在的情况flush到磁盘，同时flush返回包含对应sst的table
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else { //对builder不存在的情况，打开tableName对应sst文件
		t = &table{lm: lm, fid: fid}
		// 如果没有builder 则打开一个已经存在的sst文件
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// 先要引用一下，否则后面使用迭代器会导致引用状态错误
	t.IncrRef()

	/* 下面初始化sst对象，将主要将table index等信息加载进内存 */
	// sstable中table index信息加载进内存
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 使用tableIterator获取sst的最大key
	itr := t.NewIterator(&utils.Options{}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}
```

## 查找entry

Search方法借用tableIterator对当前sst文件检索并查找对应key的entry

```go
// Serach 从整个sstable中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	//递增递减引用计数
	t.IncrRef()
	defer t.DecrRef()
	/* 下面从table index的bloomfilter中判断key是否存在 */
	// 获取table index
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}

	/* 下面使用tableIterator检索key，并判断key是否吻合 */
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
```


## 加载特定block

block方法非常重要，当我们需要检索entry时总是需要将sstable数据读取到内存，但是sstable可能很大，我们无法直接将整个sst文件读入，因此我们每次只读取其中一个block。这也是为什么我们sst中需要存储table index，就是为了方便确定目标entry在具体哪个block中。

block方法的步骤大体为：
- 先从block cache中看有没有我们想要的block对象，有则直接返回。
- 如果缓存中没有目标block，则从SSTable的table index信息中获得对应block的offset（offsets方法），并读取block数据（read方法）
- 最后将读取到的block序列解析并构造出对应的block对象，并返回

```go
// 从sst加载索引为idx的block并返回block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}

	/* 下面从block cache中获取idx对应的block对象 */
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	/* 如果block cache中没有，下面获取BlockOffset对象并读取block字节序列 */
	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	/* 下面解析block字节序列到block对象 */
	readPos := len(b.data) - 4
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4])) // 先读取checkSum长度

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen] //读取checkSum

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4])) //读取entryOffsets长度
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd]) //读取entryOffsets

	b.entriesIndexStart = entriesIndexStart

	/* 将block加入缓存 */
	t.lm.cache.blocks.Set(key, b)

	return b, nil
}
```

上面提到的block方法中使用到了offsets函数用于从table index中提取第i个blockOffset的偏移量：

```go
// 获取sstable中第i个block的BlockOffset
func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs() //获取table index
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i] //获取BlockOffset
	return true
}
```

block方法中read函数负责从给定的block偏移量获取block的字节序列：

```go
func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}
```

blockCacheKey则和缓存相关。为了加速我们读取block过程，我们会将一些block缓存在内存中。缓存的key值是将sstable的FID拼接上block索引得到的结果，而value就是block对象。

```go
// blockCacheKey用于返回block的缓存key值，key=sstable的FID + block的idx
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// 假设t.ID没有溢出uint32
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}
```


## table属性的Get方法


```go
// 获取table当前sstable的FID
func (t *table) indexKey() uint64 {
	return t.fid
}

// 传入key字节序列和value列表的字节序列，从value列表中提取第idx个value构成entry
func (t *table) getEntry(key, block []byte, idx int) (entry *utils.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 获取sst大小
func (t *table) Size() int64 { return int64(t.ss.Size()) }

// 获取sst创建时间
func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}

// 删除sst文件
func (t *table) Delete() error {
	return t.ss.Detele()
}

// StaleDataSize返回SST中过期的数据量（他们可以在压缩时被删除）
func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }
```

## 增加减少引用计数

下面两个方法用于增加或减少引用计数。

如果仔细的话增加引用计数在上面openTable函数中被调用。此外我们每对当前table对象创建一个Iterator就会增加一个计数。我们每关闭一个Iterator则会减少一次计数。

当引用计数为0时，当前table对应sstable的entry缓存将被清除，同时sst文件将被删除。

```go
// 减少引用计数，可能删除table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// 增加引用计数
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
```


# tableIterator


前面提到的blockIterator只能遍历特定block。我们要检索整个sstable则需要在此之上提供一个tableIterator。

## 数据结构

```go
// tableIterator是table迭代器
type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table         //table对象（封装了sstable）
	blockPos int            //block索引
	bi       *blockIterator //借助blockIterator遍历block
	err      error
}

// 默认初始化并返回tableIterator
// 注意这是table的方法
func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
```

## Next

Next方法用于将迭代器指向下一entry。基本思路是先对blockIterator调用Next，如果数据无效，说明已经到当前block的末尾。此时将blockPos递增并重新调用tableIterator的Next。重新调用Next会对blockPos有效性进行检验，如果有效则用新的block初始化blockIterator。

这里注意一个问题，Next函数中总是递增的寻找下一个entry。但是Iterator的Option中有IsAsc字段用于区分是递增还是递减。递增遍历应该是从前到后的，递减遍历应该是从后到前的。这里是实现上的不足。

```go
// tableIterator的Next
// 批注：这个Next并没有区分升序（从前向后）还是降序（从后向前），总是升序遍历的
func (it *tableIterator) Next() {
	it.err = nil

	//block索引超出范围
	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	/* 下面从blockPos加载block并初始化blockIterator */
	// block中数据为空，就从blockPos位置加载block并设置blockIterator
	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}

	/* 下面调用blocIterator的Next，如果entry无效则切换到下一block并调用Next初始化新blockIterator */
	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}
```


## Seek

下面是三个一组的seek方法，用于将tableIterator定位到指定位置。seekToFirst定位到sst第一个entry。seekToLast定位到最后一个entry。Seek则定位到第一个大于等于给定key的entry。

```go
// tableIterator指向sstable中第一个entry
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	/* 加载0号block并初始化blockIterator */
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst() //blockIterator指向第一个entry
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// tableIterator指向最后一个entry
func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}

	/* 加载最后一个block并初始化blockIterator */
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast() //blockIterator指向最后一个entry
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek
// 二分法搜索key
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	//从二分法搜索第一个满足base key > key的block，获得这个block的idx
	// 注意如果key在第一个block，即blocks[0]的base key < key，那么idx会是0，而不是-1
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

// 使用blockIterator搜索key是否在blockIdx对应的block内
func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}
```


## 其他接口

```go
// tableIterator的Valid
func (it *tableIterator) Valid() bool {
	return it.err != io.EOF // 如果没有的时候 则是EOF
}

// tableIterator的Rewind：按升序降序将iterator指向首个entry
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc { //升序
		it.seekToFirst()
	} else { //降序
		it.seekToLast()
	}
}

// tableIterator的Item
func (it *tableIterator) Item() utils.Item {
	return it.it
}

// tableIterator的Close
func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}
```



































































































































































































































