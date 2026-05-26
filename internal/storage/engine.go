package storage

import (
	"log/slog"
	"sync"
)

type Engine struct {
	kv map[string]string
	mu sync.RWMutex
}

func NewEngine() *Engine {
	slog.Info("Creating new Engine instance")
	return &Engine{
		kv: make(map[string]string),
	}
}

func (e *Engine) Get(key string) (string, bool) {
	slog.Info("Get called", "key", key)
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, ok := e.kv[key]
	if ok {
		slog.Info("Key found", "key", key, "value", val)
	} else {
		slog.Warn("Key not found", "key", key)
	}
	return val, ok
}

func (e *Engine) Put(key, value string) {
	slog.Info("Put called", "key", key, "value", value)
	e.mu.Lock()
	defer e.mu.Unlock()

	e.kv[key] = value
	slog.Info("Key inserted/updated", "key", key)
}

func (e *Engine) Delete(key string) {
	slog.Info("Delete called", "key", key)
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.kv, key)
	slog.Info("Key deleted", "key", key)
}

func (e *Engine) GetAll() map[string]string {
	slog.Info("GetAll called")
	return e.kv
}
