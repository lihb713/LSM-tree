# memtable




# memtable的构成


前面已经介绍了skiplist和wal文件，这是memtable的两个主要组件，现在我们可以封装一个完整的memtable。memtable的相关操作主要有三个，分别是：
- set：将entry写入memtable的skiplist和wal文件
- get：从skiplist中获取entry
- open：从指定FID的wal文件加载entry形成新的memtable


# memtable


## 数据结构


```go
const walFileExt string = ".wal"  //wal文件后缀名

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
}
```

注意memtable的定义中包含了一个指向LSM的指针对象。尽管我们还没有介绍到LSM，但显然逻辑上memtable应该归属于一个LSM。这意味着LSM和memtable互相引用，实际中应该尽量避免这种设计。但这里我们暂时按照已有的代码实现。memtable中之所以聚合一个LSM对象，是因为一些memtable创建操作依赖于LSM某些参数，直接聚合方便我们使用这些参数。

```go
// 创建新memtable。注意这是LSM的方法
func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	//fileOpt是memtable对应wal文件的选项
	//新memtable的wal文件FID是新的累加值
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize), //TODO wal 要设置多大比较合理？ 姑且跟sst一样大
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	//使用上述选项构建一个memtable对象
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkiplist(int64(1 << 20)), lsm: lsm}
}
```


## set插入entry

```go
// 加入entry，将entry同时写入skiplist和wal文件
func (m *memTable) set(entry *utils.Entry) error {
	// 先将entry写入wal文件
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	m.sl.Add(entry)
	return nil
}
```

## get获取entry

```go
// 从skiplist中检索对应key值并返回
func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	vs := m.sl.Search(key)

	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}

	return e, nil
}
```


## recovery从wal恢复出memtable


wal主要用途就是为了在异常情况下恢复memtable，这由LSM的recovery方法实现。recovery的基本流程是：
- 将工作目录所有wal文件FID读出
- 将wal文件FID排序，因为wal的FID总是递增的，我们在上面NewMemtable方法中也能看到这点。
- 将每个wal文件对应恢复成一个memtable，更准确地说是immutables，immutable可以看作只读的memtable，满足一定条件会写入sst文件。

```go
// 将工作目录下所有wal文件解析并加载到immutables中
// 返回空的memtable和加载后的immutables
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	/* 下面将提取工作目录中所有wal后缀文件，解析文件名并提取FID，将FID存储到fids列表 */
	// 从工作目录中获取所有文件
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	// 识别后缀为.wal的文件
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) { //是否有wal后缀
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(walFileExt)], 10, 64) //取出wal后缀提取文件名，转为数字
		// 考虑 wal文件的存在 更新maxFid
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}

	// 排序一下子
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	/* 下面将所有wal文件中数据解析并恢复到immutables中 */
	imms := []*memTable{}
	// 遍历fid做处理
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			// mt.DecrRef()
			continue
		}
		// TODO 如果最后一个跳表没写满会怎么样？这不就浪费空间了吗
		imms = append(imms, mt)
	}
	// 更新最终的maxfid，初始化一定是串行执行的，因此不需要原子操作
	lsm.levels.maxFID = maxFid
	return lsm.NewMemtable(), imms
}
```


recovery方法中使用openMemTable将指定fid的wal文件加载到memtable中并返回对应memtable：

```go
// 将fid对应wal文件数据解析并构造对应memtable
// 返回构造的memtable指针
func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	//初始化wal文件选项
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	//创建memTable
	s := utils.NewSkiplist(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	//使用wal文件更新memtable的跳表
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}
```

openMemtable方法主体只是构造一个memtable对象，而将wal文件中entry写入memtable的skiplist是由UpdateSkipList实现的：

```go
// memtable使用wal文件更新跳表
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	//对wal文件中每个entry执行replayFunction
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}

	return m.wal.Truncate(int64(endOff))
}

// 将entry加入到memtable的skiplist中
func (m *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}
```


我们在介绍WalFile时已经介绍了Iterate方法。这里就是通过Iterate提取wal文件所有的entry，并对entry调用replayFunction。 replayFunction的功能就是将entry加入skiplist。


