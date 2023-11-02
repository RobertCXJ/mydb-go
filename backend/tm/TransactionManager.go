package tm

import (
	"io"
	"os"
	"sync"
)

const (
	LenXidHeaderLength = 8
	XidFieldSize       = 1
	FieldTranActive    = byte(0)
	FieldTranCommitted = byte(1)
	FieldTranAborted   = byte(2)
	SuperXid           = int64(0)
	XidSuffix          = ".xid"
)

// TransactionManager 定义了一个事务管理器接口
type TransactionManager interface {
	Begin() int64               // 开启一个新事务
	Commit(xid int64)           // 提交一个事务
	Abort(xid int64)            // 取消一个事务
	IsActive(xid int64) bool    // 查询一个事务的状态是否是正在进行的状态
	IsCommitted(xid int64) bool // 查询一个事务的状态是否是已提交
	IsAborted(xid int64) bool   // 查询一个事务的状态是否是已取消
	Close()                     // 关闭TM
}

// TransactionManagerImpl 结构体实现了 TransactionManager 接口
type TransactionManagerImpl struct {
	file        *os.File
	fc          io.WriteCloser
	counterLock sync.Mutex
	xidCounter  int64
}

func NewTransactionManagerImpl(raf *os.File, fc io.WriteCloser) *TransactionManagerImpl {
	manager := &TransactionManagerImpl{
		file:       raf,
		fc:         fc,
		xidCounter: 0,
	}
	manager.checkXIDCounter()
	return manager
}

// Create 创建一个新的 TransactionManagerImpl
func Create(path string) (TransactionManager, error) {
	filePath := path + XidSuffix

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// 写空XID文件头
	buf := make([]byte, LenXidHeaderLength)
	_, err = file.Write(buf)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &TransactionManagerImpl{file: file}, nil
}

// Open 打开一个已存在的 TransactionManagerImpl
func Open(path string) (TransactionManager, error) {
	filePath := path + XidSuffix

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &TransactionManagerImpl{file: file}, nil
}

func (t *TransactionManagerImpl) checkXIDCounter() {
	// 将文件指针移动到文件的末尾，然后返回文件的长度，并将其存储在 fileLen
	fileLen, err := t.file.Seek(0, io.SeekEnd)
	if err != nil {
		panic("BadXIDFileError")
	}
	if fileLen < LenXidHeaderLength {
		panic("BadXIDFileError")
	}

	// 分配8个字节给buf
	buf := make([]byte, LenXidHeaderLength)
	// 使用文件对象 t.file 的 ReadAt 方法，将文件的内容读取到 buf
	_, err = t.file.ReadAt(buf, 0)
	if err != nil {
		panic(err)
	}

	t.xidCounter = int64(buf[0])
	end := t.getXidPosition(t.xidCounter + 1)
	if end != fileLen {
		panic("BadXIDFileException")
	}
}

func (t *TransactionManagerImpl) getXidPosition(xid int64) int64 {
	return LenXidHeaderLength + (xid-1)*XidFieldSize
}

func (t *TransactionManagerImpl) updateXID(xid int64, status byte) {
	offset := t.getXidPosition(xid)
	tmp := []byte{status}
	_, err := t.file.WriteAt(tmp, offset)
	if err != nil {
		panic(err)
	}

	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (t *TransactionManagerImpl) incrXIDCounter() {
	t.xidCounter++
	buf := []byte{byte(t.xidCounter)}
	// 更新后的 xidCounter 写入文件的开头
	_, err := t.file.WriteAt(buf, 0)
	if err != nil {
		panic(err)
	}

	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (t *TransactionManagerImpl) Begin() int64 {
	t.counterLock.Lock()
	defer t.counterLock.Unlock()

	xid := t.xidCounter + 1
	t.updateXID(xid, FieldTranActive)
	t.incrXIDCounter()
	return xid
}

func (t *TransactionManagerImpl) Commit(xid int64) {
	t.updateXID(xid, FieldTranCommitted)
}

func (t *TransactionManagerImpl) Abort(xid int64) {
	t.updateXID(xid, FieldTranAborted)
}

func (t *TransactionManagerImpl) checkXID(xid int64, status byte) bool {
	offset := t.getXidPosition(xid)
	buf := make([]byte, XidFieldSize)
	_, err := t.file.ReadAt(buf, offset)
	if err != nil {
		panic(err)
	}
	return buf[0] == status
}

func (t *TransactionManagerImpl) IsActive(xid int64) bool {
	if xid == SuperXid {
		return false
	}
	return t.checkXID(xid, FieldTranActive)
}

func (t *TransactionManagerImpl) IsCommitted(xid int64) bool {
	if xid == SuperXid {
		return true
	}
	return t.checkXID(xid, FieldTranCommitted)
}

func (t *TransactionManagerImpl) IsAborted(xid int64) bool {
	if xid == SuperXid {
		return false
	}
	return t.checkXID(xid, FieldTranAborted)
}

func (t *TransactionManagerImpl) Close() {
	err := t.fc.Close()
	if err != nil {
		panic(err)
	}
	err = t.file.Close()
	if err != nil {
		panic(err)
	}
}
