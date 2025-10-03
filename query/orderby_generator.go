// Copyright 2022 The LUCI Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package query

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MergeWithDefaultOrder merges the specified order with the given
// defaultOrder. The merge occurs as follows:
//   - Ordering specified in `order` takes precedence.
//   - For columns not specified in the `order` that appear in `defaultOrder`,
//     ordering is applied in the order they apply in defaultOrder.
func MergeWithDefaultOrder(defaultOrder []OrderBy, order []OrderBy) []OrderBy {
	result := make([]OrderBy, 0, len(order)+len(defaultOrder))
	seenColumns := make(map[string]struct{})
	for _, o := range order {
		result = append(result, o)
		seenColumns[o.FieldPath.String()] = struct{}{}
	}
	for _, o := range defaultOrder {
		if _, ok := seenColumns[o.FieldPath.String()]; !ok {
			result = append(result, o)
		}
	}
	return result
}

// OrderByClause returns a Standard SQL Order by clause, including
// "ORDER BY" and trailing new line (if an order is specified).
// If no order is specified, returns "".
//
// The returned order clause is safe against SQL injection; only
// strings appearing from Table appear in the output.
func (t *Table) OrderByClause(order []OrderBy) (string, error) {
	if len(order) == 0 {
		return "", nil
	}
	seenColumns := make(map[string]struct{})
	var result strings.Builder
	result.WriteString("ORDER BY ")
	for i, o := range order {
		if i > 0 {
			result.WriteString(", ")
		}
		column, err := t.SortableColumnByFieldPath(o.FieldPath)
		if err != nil {
			return "", err
		}
		if _, ok := seenColumns[column.databaseName]; ok {
			return "", fmt.Errorf("field appears in order_by multiple times: %q", o.FieldPath.String())
		}
		seenColumns[column.databaseName] = struct{}{}
		result.WriteString(column.databaseName)
		if o.Descending {
			result.WriteString(" DESC")
		}
	}
	result.WriteString("\n")
	return result.String(), nil
}

// Less returns a comparator function for proto messages based on orderBy.
// The returned func(a, b) reports whether a < b according to orderBy.
func Less[M proto.Message](orderBy []OrderBy) (func(a, b M) bool, error) {
	// Validate orderBy against M’s descriptor
	var zero M
	desc := zero.ProtoReflect().Descriptor()
	for _, ob := range orderBy {
		if err := validateFieldPath(desc, ob.FieldPath.segments); err != nil {
			return nil, fmt.Errorf("invalid orderBy field %s: %w", ob.FieldPath.canonical, err)
		}
	}

	return func(a, b M) bool {
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
				return cmp > 0 // reverse for descending
			}
			return cmp < 0
		}
		return false // equal
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
