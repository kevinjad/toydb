package btree

/*
| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
*/
import (
	"bytes"
	"encoding/binary"
)

func assert(val bool) {
	if !val {
		panic("assert failed")
	}
}

const (
	HEADER             = 4
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
	BTREE_PAGE_SIZE    = 4096
)

const (
	LEAF     = 1
	INTERNAL = 0
)

type BNode []byte

type BTree struct {
	// pointer to a node
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func (bnode BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(bnode[0:2])
}

func (bnode BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(bnode[2:4])
}

func (bnode BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(bnode[0:2], btype)
	binary.LittleEndian.PutUint16(bnode[2:4], nkeys)
}

func (bnode BNode) getPtr(idx uint16) uint64 {
	assert(idx < bnode.nkeys())
	pos := HEADER + (idx * 8)
	return binary.LittleEndian.Uint64(bnode[pos:])
}

func (bnode BNode) setPtr(idx uint16, ptr uint64) {
	assert(idx < bnode.nkeys())
	pos := HEADER + (idx * 8)
	binary.LittleEndian.PutUint64(bnode[pos:], ptr)
}

func offsetPos(bnode BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= bnode.nkeys())
	return HEADER + (8 * bnode.nkeys()) + (2 * (idx - 1))
}

func (bnode BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(bnode[offsetPos(bnode, idx):])
}

func (bnode BNode) setOffset(idx uint16, offset uint16) {
	assert(idx != 0)
	binary.LittleEndian.PutUint16(bnode[offsetPos(bnode, idx):], offset)
}

func (bnode BNode) kvPos(idx uint16) uint16 {
	assert(idx <= bnode.nkeys())
	return HEADER + (8 * bnode.nkeys()) + (2 * bnode.nkeys()) + bnode.getOffset(idx)
}

func (bnode BNode) getKey(idx uint16) []byte {
	assert(idx < bnode.nkeys())
	pos := bnode.kvPos(idx)
	keylen := binary.LittleEndian.Uint16(bnode[pos:])
	return bnode[pos+4:][:keylen]
}

func (bnode BNode) getVal(idx uint16) []byte {
	assert(idx < bnode.nkeys())
	pos := bnode.kvPos(idx)
	keylen := binary.LittleEndian.Uint16(bnode[pos:])
	vlen := binary.LittleEndian.Uint16(bnode[pos+2:])
	return bnode[pos+4+keylen:][:vlen]
}

// get the size of the node
func (bnode BNode) nBytes() uint16 {
	return bnode.kvPos(bnode.nkeys())
}

////////////////////////////////////////////////////

func nodeLookUpLE(bnode BNode, key []byte) uint16 {
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	for i := 0; i < int(bnode.nkeys()); i++ {
		k := bnode.getKey(uint16(i))
		cmp := bytes.Compare(k, key)
		if cmp <= 0 {
			found = uint16(i)
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// //////todo
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// copy headers
	// copy keys values from 0 to idx
	// insert idx
	// copy keys from idx to n
	new.setHeader(LEAF, old.nkeys()+1)
	nodeAppendRangeKV(new, old, 0, 0, idx)
	// todo insert idx
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRangeKV(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// copy headers
	// copy keys values from 0 to idx
	// insert idx
	// copy keys from idx+1 to n
	new.setHeader(LEAF, old.nkeys()+1)
	nodeAppendRangeKV(new, old, 0, 0, idx)
	// todo insert idx
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRangeKV(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	//ptr
	new.setPtr(idx, ptr)
	//kv
	pos := new.kvPos(idx)
	var keylen uint16
	if key == nil {
		keylen = 0
	} else {
		keylen = uint16(len(key))
	}

	var vallen uint16
	if val == nil {
		vallen = 0
	} else {
		vallen = uint16(len(val))
	}
	binary.LittleEndian.PutUint16(new[pos:], keylen)
	binary.LittleEndian.PutUint16(new[pos+2:], vallen)
	if key != nil {
		copy(new[pos+4:], key)
	}

	if val != nil {
		copy(new[pos+4+keylen:], val)
	}
	new.setOffset(idx+1, new.getOffset(idx)+4+keylen+vallen)
}

func nodeAppendRangeKV(new BNode, old BNode, newStart uint16, oldStart uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		ni := i + newStart
		oi := i + oldStart
		key := old.getKey(oi)
		val := old.getVal(oi)
		ptr := old.getPtr(oi)
		nodeAppendKV(new, ni, ptr, key, val)
	}
}

func nodeReplaceRangeKids(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	new.setHeader(INTERNAL, old.nkeys()+uint16(len(kids))-1)
	nodeAppendRangeKV(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRangeKV(new, old, idx+uint16(len(kids)), idx+1, old.nkeys()-(idx+1))
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	if old.nBytes() <= 2*BTREE_PAGE_SIZE {
		var pos uint16
		for i := uint16(0); i < old.nkeys(); i++ {
			if old.kvPos(i) <= BTREE_PAGE_SIZE {
				pos = i
			} else {
				break
			}
		}
		left.setHeader(old.btype(), pos)
		nodeAppendRangeKV(left, old, 0, 0, pos)
		right.setHeader(old.btype(), old.nkeys()-pos)
		nodeAppendRangeKV(right, old, 0, pos, old.nkeys()-pos)
	} else {
		var pos uint16
		for i := uint16(0); i < old.nkeys(); i++ {
			if old.kvPos(i) <= 2*BTREE_PAGE_SIZE {
				pos = i
			} else {
				break
			}
		}
		left.setHeader(old.btype(), pos)
		nodeAppendRangeKV(left, old, 0, 0, pos)
		right.setHeader(old.btype(), old.nkeys()-pos)
		nodeAppendRangeKV(right, old, 0, pos, old.nkeys()-pos)
	}
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	nbytes := old.nBytes()
	if nbytes <= BTREE_PAGE_SIZE {
		return 1, [3]BNode{old}
	}
	left := make(BNode, 2*BTREE_PAGE_SIZE)
	right := make(BNode, BTREE_PAGE_SIZE)

	nodeSplit2(left, right, old)
	if left.nBytes() <= BTREE_PAGE_SIZE {
		return 2, [3]BNode{left, right}
	}
	leftleft := make(BNode, BTREE_PAGE_SIZE)
	middle := make(BNode, BTREE_PAGE_SIZE)
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nBytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := make(BNode, 2*BTREE_PAGE_SIZE)
	idx := nodeLookUpLE(node, key)
	switch node.btype() {
	case LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case INTERNAL:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node")
	}
	return new
}

func nodeInsert(tree *BTree, new BNode, old BNode, idx uint16, key []byte, val []byte) {
	kPtr := old.getPtr(idx)
	node := treeInsert(tree, tree.get(kPtr), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(kPtr)
	nodeReplaceRangeKids(tree, new, old, idx, split[:nsplit]...)
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		r := make(BNode, BTREE_PAGE_SIZE)
		r.setHeader(LEAF, 2)
		nodeAppendKV(r, 0, 0, nil, nil)
		nodeAppendKV(r, 1, 0, key, val)
		tree.root = tree.new(r)
		return
	}
	res := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(res)
	tree.del(tree.root)
	if nsplit > 1 {
		// create new root/level to accomodate the split nodes
		root := make(BNode, BTREE_PAGE_SIZE)
		root.setHeader(INTERNAL, nsplit)
		for i, node := range split[:nsplit] {
			startKey := node.getKey(0)
			nodePtr := tree.new(node)
			nodeAppendKV(root, uint16(i), nodePtr, startKey, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	var sibling BNode
	if idx > 0 {
		sibling = tree.get(node.getPtr(idx - 1))
		mergedBytes := sibling.nBytes() + updated.nBytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling = tree.get(node.getPtr(idx + 1))
		mergedBytes := sibling.nBytes() + updated.nBytes() - HEADER
		if mergedBytes <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(LEAF, old.nkeys()-1)
	nodeAppendRangeKV(new, old, 0, 0, idx)
	nodeAppendRangeKV(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRangeKV(new, left, 0, 0, left.nkeys())
	nodeAppendRangeKV(new, right, left.nkeys(), 0, right.nkeys())
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(old.btype(), old.nkeys()-1)
	nodeAppendRangeKV(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRangeKV(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	new := make(BNode, BTREE_PAGE_SIZE)
	idx := nodeLookUpLE(node, key)
	switch node.btype() {
	case LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafDelete(new, node, idx)
		} else {
			return BNode{}
		}
	case INTERNAL:
		new = nodeDelete(tree, node, idx, key)
	default:
		panic("bad node")
	}
	return new
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	ptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(ptr), key)

	if len(updated) == 0 {
		return BNode{}
	}

	tree.del(ptr)
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0) // 1 empty child but no sibling
		new.setHeader(INTERNAL, 0)            // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceRangeKids(tree, new, node, idx, updated)
	}
	return new
}
