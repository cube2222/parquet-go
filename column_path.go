package parquet

import (
	"strings"
)

// ColumnPath represents the path to a column within a parquet schema.
type ColumnPath []string

// String returns a string representation of the column path by concatenating
// each element with the "." separator.
//
// Note that path elements that contain "." separators are not escaped, which
// could result in producing an invalid representation. The output of this
// method cannot be used to reliably represent column paths as strings, it is
// only intended to provide a introspection mechanism for human users.
func (path ColumnPath) String() string {
	return strings.Join(path, ".")
}

// Append returns a new path where the given column name has been concatenated
// to the path.
func (path ColumnPath) Append(name string) ColumnPath {
	return append(path[:len(path):len(path)], name)
}

// Equal returns true if path is equal to other.
func (path ColumnPath) Equal(other ColumnPath) bool {
	return stringsAreEqual(path, other)
}

// Less returns true if path is less than other.
func (path ColumnPath) Less(other ColumnPath) bool {
	return stringsAreOrdered(path, other)
}

// HasPrefix returns true if the argument is a prefix of the column path.
func (path ColumnPath) HasPrefix(prefix ColumnPath) bool {
	return len(prefix) <= len(path) && prefix.Equal(path[:len(prefix)])
}

func stringsAreEqual(strings1, strings2 []string) bool {
	if len(strings1) != len(strings2) {
		return false
	}

	for i := range strings1 {
		if strings1[i] != strings2[i] {
			return false
		}
	}

	return true
}

func stringsAreOrdered(strings1, strings2 []string) bool {
	n := len(strings1)

	if n > len(strings2) {
		n = len(strings2)
	}

	for i := 0; i < n; i++ {
		if strings1[i] >= strings2[i] {
			return false
		}
	}

	return len(strings1) <= len(strings2)
}

type leafColumn struct {
	node               Node
	path               ColumnPath
	maxRepetitionLevel int8
	maxDefinitionLevel int8
	columnIndex        int16
}

func forEachLeafColumnOf(node Node, do func(leafColumn)) {
	forEachLeafColumn(node, nil, 0, 0, 0, do)
}

func forEachLeafColumn(node Node, path ColumnPath, columnIndex, maxRepetitionLevel, maxDefinitionLevel int, do func(leafColumn)) int {
	switch {
	case node.Optional():
		maxDefinitionLevel++
	case node.Repeated():
		maxRepetitionLevel++
		maxDefinitionLevel++
	}

	if isLeaf(node) {
		do(leafColumn{
			node:               node,
			path:               path,
			maxRepetitionLevel: makeRepetitionLevel(maxRepetitionLevel),
			maxDefinitionLevel: makeDefinitionLevel(maxDefinitionLevel),
			columnIndex:        makeColumnIndex(columnIndex),
		})
		return columnIndex + 1
	}

	for _, name := range node.ChildNames() {
		columnIndex = forEachLeafColumn(
			node.ChildByName(name),
			path.Append(name),
			columnIndex,
			maxRepetitionLevel,
			maxDefinitionLevel,
			do,
		)
	}

	return columnIndex
}

func lookupColumnPath(node Node, path ColumnPath) Node {
	for node != nil && len(path) > 0 {
		node = node.ChildByName(path[0])
		path = path[1:]
	}
	return node
}

func hasColumnPath(node Node, path ColumnPath) bool {
	return lookupColumnPath(node, path) != nil
}
