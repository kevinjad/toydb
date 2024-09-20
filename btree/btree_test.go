package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"
)

func TestCreateBNode(t *testing.T) {
	btr := make(BNode, BTREE_PAGE_SIZE)
	btr.setHeader(INTERNAL, 2)
	btr.setOffset(1, 8)
	key1 := make([]byte, 2)
	key1[0] = 1
	key1[1] = 1
	val1 := make([]byte, 2)
	val1[0] = 2
	val1[1] = 2

	pos := btr.kvPos(0)
	binary.LittleEndian.PutUint16(btr[pos:], 2)
	binary.LittleEndian.PutUint16(btr[pos+2:], 2)
	copy(btr[pos+4:], key1)
	copy(btr[pos+6:], val1)

	key1[0] = 3
	key1[1] = 3
	val1[0] = 4
	val1[1] = 4
	pos = btr.kvPos(1)
	binary.LittleEndian.PutUint16(btr[pos:], 2)
	binary.LittleEndian.PutUint16(btr[pos+2:], 2)
	copy(btr[pos+4:], key1)
	copy(btr[pos+6:], val1)

	fmt.Println(btr.getKey(0))
	fmt.Println(btr.getVal(0))
	fmt.Println(btr.getKey(1))
	fmt.Println(btr.getVal(1))
}

func TestCreateBNode2(t *testing.T) {
	node := make(BNode, 4096)
	keys := []string{"", "name2", "name3"}
	vals := []string{"", "Peter", "John"}

	node.setHeader(LEAF, 3)
	for i := 0; i < len(keys); i++ {
		nodeAppendKV(node, uint16(i), 0, []byte(keys[i]), []byte(vals[i]))
	}

	fmt.Println(string(node.getKey(0)))
	fmt.Println(string(node.getVal(0)))
	fmt.Println(string(node.getKey(1)))
	fmt.Println(string(node.getVal(1)))
	fmt.Println(string(node.getKey(2)))
	fmt.Println(string(node.getVal(2)))

}

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				assert(BNode(node).nBytes() <= BTREE_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				assert(pages[ptr] != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val // reference data
}

func TestBtreeWithInMemoryPages(t *testing.T) {
	c := newC()
	c.add("Kevin", "Abishek")
	fmt.Println("TestBtreeWithInMemoryPages completed")
}
