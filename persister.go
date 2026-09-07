package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// MemPersister keeps state in memory; survives Raft restarts within a
// process, which is what tests need to simulate a crash + recovery.
type MemPersister struct {
	mu    sync.Mutex
	state []byte
}

func (p *MemPersister) Save(b []byte) error {
	p.mu.Lock()
	p.state = append(p.state[:0], b...)
	p.mu.Unlock()
	return nil
}

func (p *MemPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

// FilePersister writes state atomically (tmp + fsync + rename) so a crash
// mid-save leaves the previous state intact.
type FilePersister struct{ Path string }

func (p FilePersister) Save(b []byte) error {
	tmp := p.Path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p.Path)
}

func (p FilePersister) Load() ([]byte, error) {
	b, err := os.ReadFile(p.Path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

func (p FilePersister) ensureDir() error { return os.MkdirAll(filepath.Dir(p.Path), 0o755) }
