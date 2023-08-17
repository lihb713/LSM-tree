# skipList代码走读


# 一个基本的skiplist

## 数据结构

```go
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64  //失效时间
}
```

```go
const (
	defaultMaxHeight = 48
)

type Element struct {
	// levels[i] 存的是这个节点的第 i 个 level 的下一个节点
	levels []*Element
	entry  *codec.Entry //实际数据
	score  float64      //用于数据比较加速
}

type SkipList struct {
	header *Element //跳表头节点指针

	rand *rand.Rand //随机数生成器

	maxLevel int //最高高度（索引值）：defaultMaxHeight - 1
	length   int
	lock     sync.RWMutex
	size     int64
}
```


## 创建跳表和节点

```go
// 创建跳表
func NewSkipList() *SkipList {

	header := &Element{
		levels: make([]*Element, defaultMaxHeight),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxHeight - 1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
```

```go
// 创建节点
func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}
```

## 跳表加速

在跳表中搜索某个key值是相当耗时的。比如key值是一个很长的字符串，那么在搜索过程中需要进行大量字符串比较。因此我们可以为key值计算一个分数用于加速比较过程。可以看作对key计算哈希值。下面给出一种实现：

```go
// 计算key对应分数
func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

// 比较key与next节点对应key值是否相等，利用score进行加速
func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)

	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}
```

## 搜索

```go
// 跳表搜索key对应元素
func (list *SkipList) Search(key []byte) (e *codec.Entry) {

	list.lock.RLock()
	defer list.lock.RUnlock()
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp := list.compare(keyScore, key, ne); comp <= 0 { //如果key>=ne.entry.Key
				if comp == 0 { //找到这个key，直接返回
					return ne.entry
				} else { //没找到，继续
					prev = ne
				}
			} else { //如果key<ne.entry.Key
				break
			}
		}
	}
	return nil
}
```

## 插入


跳表插入节点需要确定每个节点应该被插入到第几层。我们假设i层中每2个节点都会选择一个节点到i+1层。如果每次插入节点都要按此规则对整个表进行调整会非常繁琐。

我们使用一个简单策略。直接按概率计算当前节点最高应该被插入第几层。

```go
// 按每2个节点中1个节点被提升到上一层的概率，返回节点插入的层数
func (list *SkipList) randLevel() int {
	// 有 1/2 的几率返回 1
	// 有 1/4 的几率返回 2
	// 有 1/8 的几率返回 3
	// 直到最大层
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}

	return list.maxLevel
}
```

节点插入需要首先执行节点的搜索。因为跳表中节点可能被插入到不只一层中，因此我们搜索过程中需要记录每层插入位置的前驱节点。以方便最后的插入操作：

```go
// 跳表插入数据
func (list *SkipList) Add(data *codec.Entry) error {

	list.lock.Lock()
	defer list.lock.Unlock()

	//prevs记录插入节点每一层的前驱节点
	prevs := make([]*Element, list.maxLevel+1)

	key := data.Key
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
			if comp := list.compare(keyScore, key, ne); comp <= 0 { //如果key>=ne.entry.Key
				if comp == 0 { //找到这个key，直接更新数据
					ne.entry = data
					return nil
				} else { //没找到，继续
					prev = ne
				}
			} else { //如果key<ne.entry.Key
				break
			}
		}
		prevs[i] = prev //记录当前层插入节点的前驱节点
	}

	//randLevel随机生成当前节点最高插入层数
	randLevel, keyScore := list.randLevel(), list.calcScore(key)
	e := newElement(keyScore, data, randLevel)

	for i := randLevel; i >= 0; i-- {
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = e
		e.levels[i] = ne
	}
	return nil
}
```


注意当我们为skiplist插入第一个节点时，由于每一层都是空链表，即header.levels[i]=nil。因此prevs[i]值都被赋为header。因此第一个节点将被插入到header后面。














