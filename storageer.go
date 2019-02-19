package storage

// Storageer ...
type Storageer interface {
	Get(key string) (value string, err error)
	Set(key string, value string) error
	GetAllKeys() (keys []string, err error)
	GetAllKeysByPrefix(prefix string) (keys []string, err error)
	GetAll() (values map[string]string, err error)
	GetAllByPrefix(prefix string) (values map[string]string, err error)
	Delete(key string) error
	Close() error
}
