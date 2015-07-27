package themis

import "bytes"

func isContainPreservedColumn(family, qualifier []byte) bool {
	// cannot contain #
	for _, b := range family {
		if b == '#' {
			return true
		}
	}
	// check if lock column family
	if bytes.Compare(family, LockFamilyName) == 0 {
		return true
	}
	// check if put column family
	if bytes.Compare(family, PutFamilyName) == 0 {
		return true
	}
	// check if del column family
	if bytes.Compare(family, DelFamilyName) == 0 {
		return true
	}
	return false
}
