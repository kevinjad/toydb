package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
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
