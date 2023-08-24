package boltedfs

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type bfs struct {
	db bolted.Database
}

type dir struct {
	path dbpath.Path
	db   bolted.Database
	name string
}

type file struct {
	name   string
	size   int64
	reader *bytes.Reader
}

func (f file) Close() error {
	return nil
}

func (f file) Stat() (fs.FileInfo, error) {
	return f, nil
}

func (f file) Info() (fs.FileInfo, error) {
	return f, nil
}

func (f file) Name() string {
	return f.name
}

func (f file) Size() int64 {
	return f.size
}

func (f file) Mode() fs.FileMode {
	return fs.ModePerm
}

func (f file) Type() fs.FileMode {
	return fs.ModePerm
}
func (f file) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (f file) IsDir() bool {
	return false
}

func (f file) Sys() any {
	return nil
}

func (f file) Read(b []byte) (int, error) {
	return f.reader.Read(b)
}

func (d dir) Close() error {
	return fs.ErrInvalid
}

func (d dir) Stat() (fs.FileInfo, error) {
	return d, nil
}

func (d dir) Info() (fs.FileInfo, error) {
	return d, nil
}

func (d dir) Name() string {
	return d.name
}

func (d dir) Size() int64 {
	return 0
}

func (d dir) Mode() fs.FileMode {
	return fs.ModePerm | fs.ModeDir
}

func (d dir) Type() fs.FileMode {
	return fs.ModePerm | fs.ModeDir
}

func (d dir) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (d dir) IsDir() bool {
	return true
}

func (d dir) Sys() any {
	return nil
}

func (d dir) Read(b []byte) (int, error) {
	return 0, fs.ErrInvalid
}

func (d dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		n = 999
	}
	entries := []fs.DirEntry{}

	err := d.db.Read(context.Background(), func(tx bolted.ReadTx) error {
		for it := tx.Iterate(d.path); it.HasNext() && len(entries) < n; it.Next() {
			val := it.GetValue()

			k := it.GetKey()
			if val == nil {
				entries = append(entries, dir{path: d.path.Append(k), db: d.db, name: k})
				continue
			}

			entries = append(entries, file{
				name:   k,
				size:   int64(len(val)),
				reader: bytes.NewReader(val),
			})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("could not read dir %s: %w", d.path, err)
	}

	return entries, nil
}

func (f bfs) Open(name string) (fs.File, error) {
	pth, err := dbpath.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("cold not parse %s: %w", name, err)
	}

	if len(pth) == 0 {
		return dir{
			name: "/",
			path: pth,
			db:   f.db,
		}, nil
	}

	exists := false
	var data []byte
	err = f.db.Read(context.Background(), func(tx bolted.ReadTx) error {
		if !tx.Exists(pth) {
			return nil
		}

		exists = true

		if tx.IsMap(pth) {
			return nil
		}

		data = tx.Get(pth)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("could not open %s: %w", name, err)
	}

	if !exists {
		return nil, fs.ErrNotExist
	}

	fileName := "/"
	if len(pth) > 0 {
		fileName = pth[len(pth)-1]
	}

	if data != nil {
		return file{
			name:   fileName,
			size:   int64(len(data)),
			reader: bytes.NewReader(data),
		}, nil
	}

	return dir{
		name: fileName,
		path: pth,
		db:   f.db,
	}, nil

}

func New(db bolted.Database) fs.FS {
	return bfs{db: db}
}
