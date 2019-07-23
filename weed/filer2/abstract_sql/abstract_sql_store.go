package abstract_sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/journeymidnight/seaweedfs/weed/filer2"
	"github.com/journeymidnight/seaweedfs/weed/glog"
)

type AbstractSqlStore struct {
	DB               *sql.DB
	SqlInsert        string
	SqlUpdate        string
	SqlFind          string
	SqlDelete        string
	SqlListExclusive string
	SqlListInclusive string
}

type TxOrDB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (store *AbstractSqlStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, "tx", tx), nil
}
func (store *AbstractSqlStore) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Commit()
	}
	return nil
}
func (store *AbstractSqlStore) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Rollback()
	}
	return nil
}

func (store *AbstractSqlStore) getTxOrDB(ctx context.Context) TxOrDB {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx
	}
	return store.DB
}

func (store *AbstractSqlStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlInsert, hashToLong(dir), name, dir, meta)
	if err != nil {
		return fmt.Errorf("insert %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil
}

func (store *AbstractSqlStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlUpdate, meta, hashToLong(dir), name, dir)
	if err != nil {
		return fmt.Errorf("update %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil
}

func (store *AbstractSqlStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (*filer2.Entry, error) {

	dir, name := fullpath.DirAndName()
	row := store.getTxOrDB(ctx).QueryRowContext(ctx, store.SqlFind, hashToLong(dir), name, dir)
	var data []byte
	if err := row.Scan(&data); err != nil {
		return nil, filer2.ErrNotFound
	}

	entry := &filer2.Entry{
		FullPath: fullpath,
	}
	if err := entry.DecodeAttributesAndChunks(data); err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *AbstractSqlStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) error {

	dir, name := fullpath.DirAndName()

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlDelete, hashToLong(dir), name, dir)
	if err != nil {
		return fmt.Errorf("delete %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete %s but no rows affected: %s", fullpath, err)
	}

	return nil
}

func (store *AbstractSqlStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	sqlText := store.SqlListExclusive
	if inclusive {
		sqlText = store.SqlListInclusive
	}

	rows, err := store.getTxOrDB(ctx).QueryContext(ctx, sqlText, hashToLong(string(fullpath)), startFileName, string(fullpath), limit)
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var data []byte
		if err = rows.Scan(&name, &data); err != nil {
			glog.V(0).Infof("scan %s : %v", fullpath, err)
			return nil, fmt.Errorf("scan %s: %v", fullpath, err)
		}

		entry := &filer2.Entry{
			FullPath: filer2.NewFullPath(string(fullpath), name),
		}
		if err = entry.DecodeAttributesAndChunks(data); err != nil {
			glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
			return nil, fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
