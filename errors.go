package caddypgstore

import (
	"errors"
	"fmt"
	"io/fs"
)

var (
	// ErrLockNotHeld indicates that this Storage instance does not own the lock.
	ErrLockNotHeld = errors.New("lock is not held by this storage instance")
	// ErrLockLost indicates that the database lease is no longer owned by this instance.
	ErrLockLost = errors.New("lock lease was lost")
	// ErrStorageClosed indicates that the module has started cleanup.
	ErrStorageClosed = errors.New("storage is closed")
)

func notFound(kind, key string) error {
	return fmt.Errorf("%s %q: %w", kind, key, fs.ErrNotExist)
}
