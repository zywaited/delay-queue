package task

type StoreIterator interface {
	Len() int
	Next() bool
	Scan() Task
	Release()
	Rewind()
}

type StoreBaser interface {
	Add(Task) error
	Remove(string) error
	Len() int
	Get(string) Task
	Release()
}

type StoreRemover interface {
	Remove(string) error
	MultiRemove([]string) error
}

type Store interface {
	StoreBaser

	Iter() StoreIterator
}

type NewTaskStore func(level, pos int) Store
