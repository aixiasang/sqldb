package utils

import "bytes"

// CompareBytes compares two byte slices
// Returns:
// -1 if a < b
//  0 if a == b
//  1 if a > b
// Comparison is first by length, then by content
func CompareBytes(a, b []byte) int {
	// First compare by length
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	// If lengths are equal, compare by content
	return bytes.Compare(a, b)
}

// LessThan returns true if a < b in the ordering
func LessThan(a, b []byte) bool {
	return CompareBytes(a, b) < 0
}

// Equal returns true if a == b
func Equal(a, b []byte) bool {
	return CompareBytes(a, b) == 0
}

// GreaterThan returns true if a > b in the ordering
func GreaterThan(a, b []byte) bool {
	return CompareBytes(a, b) > 0
}
