package query

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Comparer returns a comparator function for proto messages based on orderBy.
// The returned func(a, b) returns <0 if a < b, 0 if equal, >0 if a > b.
func Comparer[M proto.Message](orderBy []OrderBy) (func(a, b M) int, error) {
	// Validate orderBy against M's descriptor (same as in Less).
	var zero M
	desc := zero.ProtoReflect().Descriptor()
	for _, ob := range orderBy {
		if err := validateFieldPath(desc, ob.FieldPath.segments); err != nil {
			return nil, fmt.Errorf("invalid orderBy field %s: %w", ob.FieldPath.canonical, err)
		}
	}

	return func(a, b M) int {
		am := a.ProtoReflect()
		bm := b.ProtoReflect()

		for _, ob := range orderBy {
			av, _ := getFieldPathValue(am, ob.FieldPath.segments)
			bv, _ := getFieldPathValue(bm, ob.FieldPath.segments)

			cmp := compareValues(av, bv)
			if cmp == 0 {
				continue
			}
			if ob.Descending {
				// Reverse the sign for descending.
				return -cmp
			}
			return cmp
		}
		return 0 // All fields compared equal
	}, nil
}

// Less returns a comparator function for proto messages based on orderBy.
// The returned func(a, b) reports whether a < b according to orderBy.
func Less[M proto.Message](orderBy []OrderBy) (func(a, b M) bool, error) {
	cmp, err := Comparer[M](orderBy)
	if err != nil {
		return nil, err
	}
	return func(a, b M) bool {
		// a < b if comparator(a, b) is negative
		return cmp(a, b) < 0
	}, nil
}

// validateFieldPath walks the descriptor to make sure segments are valid.
func validateFieldPath(desc protoreflect.MessageDescriptor, segments []string) error {
	for _, seg := range segments {
		fd := desc.Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return fmt.Errorf("field %s not found on %s", seg, desc.FullName())
		}
		if fd.Cardinality() == protoreflect.Repeated {
			return fmt.Errorf("cannot sort on repeated field %s in message %s", seg, desc.FullName())
		}
		if fd.Message() != nil {
			desc = fd.Message()
		}
	}
	return nil
}

// getFieldPathValue walks down nested fields along segments.
func getFieldPathValue(m protoreflect.Message, segments []string) (protoreflect.Value, error) {
	for i, seg := range segments {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(seg))
		if fd == nil {
			return protoreflect.Value{}, fmt.Errorf("field %s not found", seg)
		}
		val := m.Get(fd)
		if i == len(segments)-1 {
			return val, nil
		}
		if fd.Message() == nil || !m.Has(fd) {
			return protoreflect.Value{}, fmt.Errorf("field %s is not a message", seg)
		}
		m = val.Message()
	}
	return protoreflect.Value{}, fmt.Errorf("empty segments")
}

// compareValues performs an ordering comparison between two protoreflect.Values.
// Returns -1 if a < b, 0 if equal, +1 if a > b.
func compareValues(a, b protoreflect.Value) int {
	switch av := a.Interface().(type) {
	case int32:
		bv := b.Interface().(int32)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	case int64:
		bv := b.Interface().(int64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	case uint32:
		bv := b.Interface().(uint32)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	case uint64:
		bv := b.Interface().(uint64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	case string:
		bv := b.Interface().(string)
		return strings.Compare(av, bv)
	case bool:
		bv := b.Interface().(bool)
		switch {
		case av == bv:
			return 0
		case !av && bv:
			return -1
		default:
			return 1
		}
	case nil:
		if b.Interface() != nil {
			return -1
		} else {
			return 1
		}
	default:
		// TODO: extend with other scalar types (enums, bytes, timestamps, etc.)
		panic(fmt.Sprintf("unsupported type %T in compareValues", av))
	}
}
