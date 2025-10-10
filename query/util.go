package query

import "strings"

// quoteLike turns a literal string into an escaped like expression.
// This means strings like test_name will only match as expected, rather than
// also matching test3name.
func quoteLike(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "%", "\\%")
	value = strings.ReplaceAll(value, "_", "\\_")
	return value
}
