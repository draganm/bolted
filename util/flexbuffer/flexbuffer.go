package flexbuffer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
)

type Flexbuffer struct {
	head        *bytes.Buffer
	tail        *os.File
	headMaxSize int
	TotalSize   int
}

func New(headMaxSize int) *Flexbuffer {
	return &Flexbuffer{
		head:        new(bytes.Buffer),
		headMaxSize: headMaxSize,
	}
}

func (f *Flexbuffer) Write(d []byte) (written int, err error) {
	remainingHeadSpace := f.headMaxSize - f.head.Len()

	defer func() {
		f.TotalSize += written
	}()

	if len(d) < remainingHeadSpace {
		remainingHeadSpace = len(d)
	}

	hw, err := f.head.Write(d[:remainingHeadSpace])
	written += hw

	if err != nil {
		return written, err
	}

	if len(d[remainingHeadSpace:]) == 0 {
		return written, nil
	}

	if f.tail == nil {
		tf, err := os.CreateTemp("", "*")
		if err != nil {
			return written, fmt.Errorf("while creating tail: %w", err)
		}
		f.tail = tf
		runtime.SetFinalizer(tf, func(tf *os.File) {
			tf.Close()
			os.Remove(tf.Name())
		})
	}

	tw, err := f.tail.Write(d[remainingHeadSpace:])
	written += tw
	if err != nil {
		return written, err
	}

	return written, nil
}

func (f *Flexbuffer) FinishWrite() error {
	if f.tail != nil {
		_, err := f.tail.Seek(0, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Flexbuffer) Read(p []byte) (n int, err error) {
	if f.head.Len() > 0 {
		return f.head.Read(p)
	}
	if f.tail != nil {
		r, err := f.tail.Read(p)
		if err == io.EOF {
			runtime.SetFinalizer(f, nil)
			f.tail.Close()
			os.Remove(f.tail.Name())
		}
		return r, err
	}

	return 0, io.EOF

}
