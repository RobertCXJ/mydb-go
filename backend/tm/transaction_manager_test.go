package tm

import (
	"fmt"
	"os"
	"testing"
)

func TestTransactionManager(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	//defer os.Remove(path + XidSuffix)

	// 测试 Begin、Commit 和 Abort
	xid := tm.Begin()
	if !tm.IsActive(xid) {
		t.Errorf("Expected IsActive(xid) to be true")
	}

	if tm.IsActive(xid) {
		fmt.Println("事务活跃")
	}
	tm.Commit(xid)
	if !tm.IsCommitted(xid) {
		t.Errorf("Expected IsCommitted(xid) to be true")
	}
	if tm.IsCommitted(xid) {
		fmt.Println("事务提交")
	}
	tm.Abort(xid)
	if !tm.IsAborted(xid) {
		t.Errorf("Expected IsAborted(xid) to be true")
	}
	if tm.IsAborted(xid) {
		fmt.Println("事务取消")
	}

	xidTest := tm.xidCounter + 1
	tm.updateXID(xidTest, FieldTranActive)
	tm.incrXIDCounter()

	tm.Commit(xidTest)

	fmt.Println(xidTest)

	// Close the transaction manager
	defer tm.Close()

	// Reopen the transaction manager
	tm2, err := Open(path)
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
	defer tm2.Close()

	tm2.Begin()

	fmt.Println(tm2.xidCounter)

	// Check if the transaction manager reopens successfully
	if tm2 == nil {
		t.Errorf("Transaction manager not reopened")
	}

	// Check if the transaction is still committed after reopening
	if !tm2.IsActive(tm2.xidCounter) {
		t.Errorf("Transaction not marked as committed after reopening")
	}

}

func TestIncrXIDCounter(t *testing.T) {
	// 创建测试目录
	err := os.MkdirAll("D:\\data\\db", 0755)
	if err != nil {
		t.Fatal("Failed to create test directory:", err)
	}

	// 创建一个 TransactionManager
	tm, err := Create("D:\\data\\db\\test_tm")
	if err != nil {
		t.Fatal("Failed to create TransactionManager:", err)
	}
	defer os.Remove("D:\\data\\db\\test_tm.xid")

	// 测试 incrXIDCounter
	tm.incrXIDCounter()
	if tm.xidCounter != 1 {
		t.Errorf("Expected xidCounter to be 1, but got %d", tm.xidCounter)
	}

	// 进行其他测试逻辑
	// ...
}

// 其他版本

func TestCreate(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	// Check if the file was created
	_, err = os.Stat(path + XidSuffix)
	if err != nil {
		t.Errorf("File not created: %v", err)
	}

	tm.Begin()

	// Check the initial state of XID counter
	if tm.xidCounter != 1 {
		t.Errorf("XID counter not initialized correctly")
	}
}

func TestOpen(t *testing.T) {
	path := "test_file"
	_, err := os.Create(path + XidSuffix)
	if err != nil {
		t.Errorf("Test setup failed: %v", err)
	}
	tm, err := Open(path)
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	// Check if the file was opened successfully
	if tm == nil {
		t.Errorf("Transaction manager not created")
	}
}

func TestCheckXIDCounter(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	tm.Begin()
	tm.checkXIDCounter()

	// Check if XID counter is initialized to 1
	if tm.xidCounter != 1 {
		t.Errorf("XID counter not initialized correctly")
	}
}

func TestXidPosition(t *testing.T) {
	// 测试 getXidPosition
	tm := &TransactionManagerImpl{}
	xid := int64(123)
	expectedPosition := int64(LenXidHeaderLength + (xid-1)*XidFieldSize)
	position := tm.getXidPosition(xid)
	if position != expectedPosition {
		t.Errorf("Expected position to be %d, but got %d", expectedPosition, position)
	}
}

func TestUpdateXID(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	tm.Begin()
	xid := tm.xidCounter + 1

	status := FieldTranCommitted
	tm.updateXID(xid, status)

	// Check if the status of the transaction was updated correctly
	if !tm.checkXID(xid, status) {
		t.Errorf("XID status not updated correctly")
	}
}

func TestBegin(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()

	// Check if the XID was incremented and marked as active
	if xid != 1 {
		t.Errorf("XID not incremented correctly")
	}
	if !tm.IsActive(xid) {
		t.Errorf("XID not marked as active")
	}
}

func TestCommit(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()
	tm.Commit(xid)

	// Check if the XID is marked as committed
	if !tm.IsCommitted(xid) {
		t.Errorf("XID not marked as committed")
	}
}

func TestAbort(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()
	tm.Abort(xid)

	// Check if the XID is marked as aborted
	if !tm.IsAborted(xid) {
		t.Errorf("XID not marked as aborted")
	}
}

func TestCheckXID(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()
	if !tm.checkXID(xid, FieldTranActive) {
		t.Errorf("XID status not checked correctly")
	}

	tm.Commit(xid)
	// Check if the XID status is correctly reported
	if !tm.checkXID(xid, FieldTranCommitted) {
		t.Errorf("XID status not checked correctly")
	}

	tm.Abort(xid)
	if !tm.checkXID(xid, FieldTranAborted) {
		t.Errorf("XID status not checked correctly")
	}

}

func TestIsActive(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()

	// Check if the XID is marked as active
	if !tm.IsActive(xid) {
		t.Errorf("XID not marked as active")
	}
}

func TestIsCommitted(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()
	tm.Commit(xid)

	// Check if the XID is marked as committed
	if !tm.IsCommitted(xid) {
		t.Errorf("XID not marked as committed")
	}
}

func TestIsAborted(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

	xid := tm.Begin()
	tm.Abort(xid)

	// Check if the XID is marked as aborted
	if !tm.IsAborted(xid) {
		t.Errorf("XID not marked as aborted")
	}
}

func TestClose(t *testing.T) {
	path := "test_file"
	tm, err := Create(path)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}
	defer os.Remove(path + XidSuffix)
	defer tm.Close()

}
