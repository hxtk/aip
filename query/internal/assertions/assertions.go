package assertions

import (
	"fmt"

	"github.com/smarty/assertions"
)

// ShouldErrLike compares an `error` or `string` on the left side, to `error`s
// or `string`s on the right side.
//
// If multiple errors/strings are provided on the righthand side, they must all
// be contained in the stringified error on the lefthand side.
//
// If the righthand side is the singluar `nil`, this expects the error to be
// nil.
//
// Example:
//
//	// Usage                          Equivalent To
//	So(err, ShouldErrLike, "custom")    // `err.Error()` ShouldContainSubstring "custom"
//	So(err, ShouldErrLike, io.EOF)      // `err.Error()` ShouldContainSubstring io.EOF.Error()
//	So(err, ShouldErrLike, "EOF")       // `err.Error()` ShouldContainSubstring "EOF"
//	So(err, ShouldErrLike,
//	   "thing", "other", "etc.")        // `err.Error()` contains all of these substrings.
//	So(nilErr, ShouldErrLike, nil)      // nilErr ShouldBeNil
//	So(nonNilErr, ShouldErrLike, "")    // nonNilErr ShouldNotBeNil
func ShouldErrLike(actual any, expected ...any) string {
	if len(expected) == 0 {
		return "ShouldErrLike requires 1 or more expected values, got 0"
	}
	// If we have multiple expected arguments, they must all be non-nil
	if len(expected) > 1 {
		for _, e := range expected {
			if e == nil {
				return "ShouldErrLike only accepts `nil` on the right hand side as the sole argument."
			}
		}
	}
	if expected[0] == nil { // this can only happen if len(expected) == 1
		return assertions.ShouldBeNil(actual)
	} else if actual == nil {
		return assertions.ShouldNotBeNil(actual)
	}
	ae, ok := actual.(error)
	if !ok {
		return assertions.ShouldImplement(actual, (*error)(nil))
	}
	for _, expect := range expected {
		switch x := expect.(type) {
		case string:
			if ret := assertions.ShouldContainSubstring(ae.Error(), x); ret != "" {
				return ret
			}
		case error:
			if ret := assertions.ShouldContainSubstring(ae.Error(), x.Error()); ret != "" {
				return ret
			}
		default:
			return fmt.Sprintf("unexpected argument type %T, expected string or error", expect)
		}
	}
	return ""
}
