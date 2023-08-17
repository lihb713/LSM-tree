# level





# 概述


到此为止我们已经有了memtable、sstable等基本数据存储对象。同时还有manifest做sstable文件管理、一整个的iterator体系来遍历memtable、sstable等数据存储对象。我们已经具备了所有构成LSM-tree所需的组件。如果将系统分为基本构成和策略两个层次的话，那么我们现在可以开始考虑策略相关的内容了。

LSM-tree的策略就是我们应该如何管理存储在硬盘的sst文件。在LSM-tree中sst文件被组织成一定层次结构。首先是第0层，是memtable被直接刷写到磁盘中的层次。但是将memtable直接写入磁盘虽然提高写效率，但是由于不同sst文件之间key可能互相重叠，因此查找非常低效。于是就有了1~N层，我们通过归并排序的方式将第x层sst文件合并到第y层，这使得数据更加有序，大大提高查询速度。同时由于LSM-tree总是以追加方式写入数据，可能存在相同key的不同版本，这种合并还能帮助我们压缩掉无效的数据。我们将这种将某一层sst文件合并到另一层的操作称为compact。显然compact需要考虑很多问题？如什么时候进行合并，选取哪些sst文件进行合并，合并的结果放在哪一层等等，因此整个compact操作构成了我们LSM-tree的策略层。

以上是LSM-tree中compact操作的基本介绍。要想实现compact我们需要一个层级间管理table的管理器，我们称之为levelManager。本节我们暂且不介绍levelManager，而是介绍它的一个重要组件levelHandler。

# levelHandler


levalHandler作用是对某一层次的sstable进行管理。其中维护了一个当前层table对象列表，以及table总大小。我们可以增加、减少或修改levelHandler中的table对象列表，也可以进行排序以及在当前层搜索获取某条entry。


## 数据结构

```go
// levelHandler管理一层的sst文件
type levelHandler struct {
	sync.RWMutex            //读写锁
	levelNum       int      //层级
	tables         []*table //sst表
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}
```

## 增加删除table

下面提供了一组用于修改当前levelHandler的table列表函数。

replaceTables和deleteTables有类似调用方式，都是将当前层table位于toDel中的table删除，并且讲toAdd中的table加入。

add和addBatch方法则是将给定的table或table列表插入。

注意我们在修改levelHandler的table列表时进行的是写操作，因此需要加写锁。

```go
// replaceTables将tables[left:right]替换为newTables，注意不包括tables[right]。
// 你必须在向manifest写入更新之后调用decr()删除旧的tables
func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {

	// 需要重新搜索此级别中需要替换的表的范围，因为其他go程可能也在更改它
	// 其他go程不能碰我们的表，如果它们添加或删除其他表，索引就会发生变化
	lh.Lock() // 我们在下面Unlock

	/* 下面将table列表toDel转换成集合toDelMap */
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	/* 下面将levelHandler对应层的table不在toDelMap中的加入newTables */
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}

	/* 下面将toAdd加入newTables */
	for _, t := range toAdd {
		lh.addSize(t)
		t.IncrRef()
		newTables = append(newTables, t)
	}

	/* 下面将levelHandler的tables设置为newTables并排序 */
	lh.tables = newTables
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[i].ss.MinKey()) < 0
	})
	lh.Unlock() //在DecrRef表之前Unlock
	return decrRefs(toDel)
}

// deleteTables将删除当前层在toDel列表中的table
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // 下面Unlock

	/* 下面将table列表toDel转换成集合toDelMap */
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	/* 下面将levelHandler对应层的table不在toDelMap中的加入newTables */
	// 制作副本，因为迭代器可能保留一部分表
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables

	lh.Unlock() //在DecrRef表之前Unlock

	return decrRefs(toDel)
}

// 增加一个table
func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

// 批量增加table
func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
}
```

在我们增加删除table时需要同时更新levelHandler中记录的table大小：

```go
// 返回totalSize
func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}

// 计算totalSize和totalStaleSize
func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

// 计算减去给定table后的totalSize和totalStaleSize
func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
}
```


## Sort对table排序

Sort函数对当前层的table按键值进行排序。注意这里要区分当前层是0层还是非0层。

0层由于本身key值无序，存在重叠情况，因此直接按sst的FID排序即可。

非0层则按照key的大小进行排序。

```go
// 将levelHandler对应层的table进行排序
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 { //0层，Key范围将重叠，只需按照fileID升序排序
		// 较新的表位于级别0的末尾
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else { //非0层，按key排序
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}
```



## Get获取entry

levelHandler提供了Get方法用于从当前层的所有sst中获取给定key的entry。

同样的，我们应该分情况处理。对于0层由于不同sst的key可能重叠，我们不得不遍历所有sst。对于非0层由于key有序，我们只需要在包含key的sst文件中搜索即可。

```go
// 给定key，从当前层sst中返回对应entry
func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 { // 如果是第0层文件则进行特殊处理
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

// 给定key，搜索0层table并返回对应entry
func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// 给定key，搜索非0层table并返回对应entry
func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key) //先获取key对应table
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Serach(key, &version); err == nil { //再调用table的Search方法获取key对应entry
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 给定key，返回包含key的table
func (lh *levelHandler) getTable(key []byte) *table {
	if len(lh.tables) > 0 && (bytes.Compare(key, lh.tables[0].ss.MinKey()) < 0 || bytes.Compare(key, lh.tables[len(lh.tables)-1].ss.MaxKey()) > 0) {
		return nil
	} else {
		for i := len(lh.tables) - 1; i >= 0; i-- {
			if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
				bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
				return lh.tables[i]
			}
		}
	}
	return nil
}
```

## 查找某一keyRange的table

keyRange是表示某一个key范围的对象，其包含left、right两个成员指定key的范围。我们在讲解compact时会详细说明。

下面方法给定keyRange对象并返回位于这一key范围的table在当前层的索引范围。我们假设最终得到的table为tableMin和tableMax，那么有keyRange.left\<=tableMin.maxKey，keyRange.right\<tableMax.maxkey。

简单来说，overlappingTables从当前层找到table范围，覆盖了keyRange的key的范围。

```go
// 返回key范围相交的表，Returns a half-interval.
// 这个函数应该已经获取了读锁，这一点很重要，调用方必须传递一个声明读锁的空参数
func (lh *levelHandler) overlappingTables(_ levelHandlerRLocked, kr keyRange) (int, int) {
	if len(kr.left) == 0 || len(kr.right) == 0 {
		return 0, 0
	}
	left := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.left, lh.tables[i].ss.MaxKey()) <= 0
	})
	right := sort.Search(len(lh.tables), func(i int) bool {
		return utils.CompareKeys(kr.right, lh.tables[i].ss.MaxKey()) < 0
	})
	return left, right
}
```


## 迭代器

iterators范围一个迭代器序列表示一层的迭代器。对于0层，由于无序，我们直接返回0层各个table的迭代器即可。对于非0层，key有序，我们将构建一个ConcatIterator。

```go
// 从levelHandler中获取当前层table对应的Iterator列表（对于非0层来说，会是一个ConcatIterator）
func (lh *levelHandler) iterators() []utils.Iterator {
	lh.RLock()
	defer lh.RUnlock()
	topt := &utils.Options{IsAsc: true}
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables, topt)
	}

	if len(lh.tables) == 0 {
		return nil
	}
	return []utils.Iterator{NewConcatIterator(lh.tables, topt)}
}
```

## 其他

```go
// 关闭levelHandler中所有table，即关闭mmp映射及对应sst文件句柄
func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 返回table数量
func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

// 这个levelHandler是否在最后一层
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}
```




































































































































































































































