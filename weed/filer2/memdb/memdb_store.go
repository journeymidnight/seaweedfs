package memdb

import (
	"context"
	"fmt"
	"github.com/google/btree"
	"github.com/journeymidnight/seaweedfs/weed/filer2"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"strings"
	"sync"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MemDbStore{})
}

type MemDbStore struct {
	tree     *btree.BTree
	treeLock sync.Mutex
}

type entryItem struct {
	*filer2.Entry
}

func (a entryItem) Less(b btree.Item) bool {
	return strings.Compare(string(a.FullPath), string(b.(entryItem).FullPath)) < 0
}

func (store *MemDbStore) GetName() string {
	return "memory"
}

func (store *MemDbStore) Initialize(configuration util.Configuration) (err error) {
	store.tree = btree.New(8)
	return nil
}

func (store *MemDbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *MemDbStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *MemDbStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *MemDbStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	// println("inserting", entry.FullPath)
	store.treeLock.Lock()
	store.tree.ReplaceOrInsert(entryItem{entry})
	store.treeLock.Unlock()
	return nil
}

func (store *MemDbStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	if _, err = store.FindEntry(ctx, entry.FullPath); err != nil {
		return fmt.Errorf("no such file %s : %v", entry.FullPath, err)
	}
	store.treeLock.Lock()
	store.tree.ReplaceOrInsert(entryItem{entry})
	store.treeLock.Unlock()
	return nil
}

func (store *MemDbStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	item := store.tree.Get(entryItem{&filer2.Entry{FullPath: fullpath}})
	if item == nil {
		return nil, filer2.ErrNotFound
	}
	entry = item.(entryItem).Entry
	return entry, nil
}

func (store *MemDbStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) (err error) {
	store.treeLock.Lock()
	store.tree.Delete(entryItem{&filer2.Entry{FullPath: fullpath}})
	store.treeLock.Unlock()
	return nil
}

func (store *MemDbStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	startFrom := string(fullpath)
	if startFileName != "" {
		startFrom = startFrom + "/" + startFileName
	}

	store.tree.AscendGreaterOrEqual(entryItem{&filer2.Entry{FullPath: filer2.FullPath(startFrom)}},
		func(item btree.Item) bool {
			if limit <= 0 {
				return false
			}
			entry := item.(entryItem).Entry
			// println("checking", entry.FullPath)

			if entry.FullPath == fullpath {
				// skipping the current directory
				// println("skipping the folder", entry.FullPath)
				return true
			}

			dir, name := entry.FullPath.DirAndName()
			if name == startFileName {
				if inclusive {
					limit--
					entries = append(entries, entry)
				}
				return true
			}

			// only iterate the same prefix
			if !strings.HasPrefix(string(entry.FullPath), string(fullpath)) {
				// println("breaking from", entry.FullPath)
				return false
			}

			if dir != string(fullpath) {
				// this could be items in deeper directories
				// println("skipping deeper folder", entry.FullPath)
				return true
			}
			// now process the directory items
			// println("adding entry", entry.FullPath)
			limit--
			entries = append(entries, entry)
			return true
		},
	)
	return entries, nil
}
