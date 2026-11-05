package raft

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// MemPersister keeps state in memory; survives Raft restarts within a
// process, which is what tests need to simulate a crash + recovery.
type MemPersister struct {
	mu    sync.Mutex
	state []byte
}

// Save implements Persister. It keeps a copy of b and never fails.
func (p *MemPersister) Save(b []byte) error {
	p.mu.Lock()
	p.state = append(p.state[:0], b...)
	p.mu.Unlock()
	return nil
}

// Load implements Persister, returning a copy of the last saved state, or
// nil if nothing was saved.
func (p *MemPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

// FilePersister writes state atomically and durably: write a temp file, fsync
// it, rename it over the old file, then fsync the directory so the rename
// itself survives a power loss. A crash at any point leaves either the old or
// the new state, and Save returns nil only once the new state is on disk.
type FilePersister struct{ Path string }

// Save implements Persister. It writes b to Path+".tmp", fsyncs it, renames
// it over Path and fsyncs the directory, and returns nil only once all of that
// succeeded. The directory must already exist; Save does not create it.
func (p FilePersister) Save(b []byte) error {
	tmp := p.Path + ".tmp"
	f, err := os.Create(tmp) //nolint:gosec // G304: the path is the embedder's configured state file, not untrusted input
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.Path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(p.Path))
}

// syncDir makes a rename in dir durable. Windows cannot fsync a directory
// (and NTFS journals renames), so it is skipped there.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir) //nolint:gosec // G304: dir is the parent of the embedder's configured state file
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		return errors.Join(err, d.Close())
	}
	return d.Close()
}

// Load implements Persister. A missing file means a node that never saved
// anything, so it returns nil, nil. A Path+".tmp" left by a crash during Save
// is ignored; the next Save overwrites it.
func (p FilePersister) Load() ([]byte, error) {
	b, err := os.ReadFile(p.Path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}
