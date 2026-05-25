package engine

import "sync"

type Engine struct {
	kv map[string]string
	mu sync.RWMutex
}

func New() *Engine {
	return &Engine{
		kv: make(map[string]string),
	}
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, ok := e.kv[key]
	return val, ok
}

func (e *Engine) Put(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.kv[key] = value
}

func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.kv, key)
}
