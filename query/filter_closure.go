package query

import (
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtoFilter compiles a parsed AIP-160 Filter into a type-safe predicate function.
//
// Example:
//
//	f, err := aip.ProtoFilter[testpb.Book](aip.MustParseFilter(`author.family_name = "Hunt"`))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	ok := f(book) // book satisfies filter?
//
// The returned closure never returns an error: all validation occurs at construction.
func ProtoFilter[S any, M interface {
	proto.Message
	*S
}](f *Filter) (func(M) bool, error) {
	if f == nil {
		// empty filter always true
		return func(M) bool { return true }, nil
	}

	// Construct a zero instance of the target message type so validation can
	// detect field lookup or type errors up front.
	var zeroRaw S
	var zero M = &zeroRaw

	// Perform validation once; discard result.
	if _, err := matchesFilter(zero, f); err != nil {
		return nil, err
	}

	// Return a pure boolean predicate closure.
	return func(m M) bool {
		ok, _ := matchesFilter(m, f)
		return ok
	}, nil
}

// matchesFilter returns true if msg satisfies the filter expression.
// Empty filter matches everything.
func matchesFilter(msg proto.Message, f *Filter) (bool, error) {
	if f == nil || f.Expression == nil {
		return true, nil
	}
	return evalExpression(msg.ProtoReflect(), f.Expression)
}

// ---- AST evaluation (AND/OR/NOT/parentheses) ----

func evalExpression(m protoreflect.Message, e *Expression) (bool, error) {
	for _, seq := range e.Sequences {
		ok, err := evalSequence(m, seq)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalSequence(m protoreflect.Message, s *Sequence) (bool, error) {
	for _, f := range s.Factors {
		ok, err := evalFactor(m, f)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalFactor(m protoreflect.Message, f *Factor) (bool, error) {
	for _, t := range f.Terms {
		ok, err := evalTerm(m, t)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func evalTerm(m protoreflect.Message, t *Term) (bool, error) {
	ok, err := evalSimple(m, t.Simple)
	if err != nil {
		return false, err
	}
	if t.Negated {
		return !ok, nil
	}
	return ok, nil
}

func evalSimple(m protoreflect.Message, s *Simple) (bool, error) {
	if s.Restriction != nil {
		return evalRestriction(m, s.Restriction)
	}
	if s.Composite != nil {
		return evalExpression(m, s.Composite)
	}
	return false, fmt.Errorf("invalid simple node")
}

// ---- restriction evaluation ----

func evalRestriction(m protoreflect.Message, r *Restriction) (bool, error) {
	// Case 1: global restriction — no comparator.
	if r.Comparator == "" {
		term := r.Comparable.Member.Value
		return searchMessageStrings(m, term), nil
	}

	// Case 2: normal comparator-based restriction.
	lhs, err := resolveMemberValue(m, r.Comparable.Member)
	if err != nil {
		return false, err
	}
	if r.Arg == nil {
		return false, fmt.Errorf("missing arg in restriction")
	}
	rhs, err := resolveMemberValue(m, r.Arg.Comparable.Member)
	if err != nil {
		return false, err
	}
	return compareAny(lhs, rhs, r.Comparator)
}

func searchMessageStrings(m protoreflect.Message, term string) bool {
	term = strings.ToLower(term)
	desc := m.Descriptor()
	fields := desc.Fields()

	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		val := m.Get(fd)

		switch {
		case fd.IsList():
			l := val.List()
			for j := 0; j < l.Len(); j++ {
				if fieldContains(fd, l.Get(j), term) {
					return true
				}
			}

		case fd.IsMap():
			mp := val.Map()
			found := false
			mp.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				if fieldContains(fd.MapKey(), protoreflect.ValueOf(k.Interface()), term) ||
					fieldContains(fd.MapValue(), v, term) {
					found = true
					return false
				}
				return true
			})
			if found {
				return true
			}

		default:
			if fieldContains(fd, val, term) {
				return true
			}
		}
	}
	return false
}

func fieldContains(fd protoreflect.FieldDescriptor, v protoreflect.Value, term string) bool {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return strings.Contains(strings.ToLower(v.String()), term)
	case protoreflect.MessageKind:
		if v.Message().IsValid() {
			return searchMessageStrings(v.Message(), term)
		}
	}
	return false
}

// ---- resolving member -> runtime value ----
//
// Behavior notes:
//  * If the top-level member name doesn't exist as a field on the current
//    message, we treat the Member as a literal string (so `"Hunt"` or unresolvable
//    token becomes a literal).
//  * Repeated message fields can be descended into: e.g. `authors.family_name`
//    returns a []any of that subfield for each element. Comparison
//    semantics treat slices as "any element matches" for =, :, !=, etc.
//  * Maps are returned as map[any]any for simple membership tests.

func resolveMemberValue(m protoreflect.Message, mem *Member) (any, error) {
	// Try to find the top-level field descriptor by name.
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(mem.Value))
	if fd == nil {
		// No such field -> treat as literal token (string).
		if len(mem.Fields) > 0 {
			return nil, fmt.Errorf("unknown top-level field %q", mem.Value)
		}
		return mem.Value, nil
	}

	val := m.Get(fd)

	// Map
	if fd.IsMap() {
		mp := make(map[any]any)
		mv := val.Map()
		mv.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			mp[k.Interface()] = v.Interface()
			return true
		})
		if len(mem.Fields) == 0 {
			return mp, nil
		}
		return nil, fmt.Errorf("cannot descend into map field %q", mem.Value)
	}

	// Repeated (list)
	if fd.IsList() {
		l := val.List()
		// If no further fields, return slice of raw elements.
		if len(mem.Fields) == 0 {
			out := make([]any, l.Len())
			for i := 0; i < l.Len(); i++ {
				out[i] = l.Get(i).Interface()
			}
			return out, nil
		}
		// If the list element is a message and fields follow, return []any
		// where each element is the resolved subfield for that element (or nil).
		if fd.Message() == nil {
			return nil, fmt.Errorf("cannot descend into repeated non-message field %q", mem.Value)
		}
		var results []any
		for i := 0; i < l.Len(); i++ {
			elemMsg := l.Get(i).Message()
			if !elemMsg.IsValid() {
				results = append(results, nil)
				continue
			}
			sub, err := resolveMemberValueFromMessage(elemMsg, mem.Fields)
			if err != nil {
				return nil, err
			}
			results = append(results, sub)
		}
		return results, nil
	}

	// Singular message/primitive
	if len(mem.Fields) == 0 {
		return val.Interface(), nil
	}
	// Descend into submessage fields.
	if fd.Message() == nil {
		return nil, fmt.Errorf("cannot descend into non-message field %q", mem.Value)
	}
	subMsg := val.Message()
	if !subMsg.IsValid() {
		// missing message -> treat as nil
		return nil, nil
	}
	return resolveMemberValueFromMessage(subMsg, mem.Fields)
}

func resolveMemberValueFromMessage(m protoreflect.Message, fields []string) (any, error) {
	cur := m
	for i, fname := range fields {
		fd := cur.Descriptor().Fields().ByName(protoreflect.Name(fname))
		if fd == nil {
			return nil, fmt.Errorf("unknown subfield %q", fname)
		}
		v := cur.Get(fd)
		// If last field, return interface / slice / map as appropriate
		if i == len(fields)-1 {
			if fd.IsMap() {
				mp := make(map[any]any)
				v.Map().Range(func(k protoreflect.MapKey, vv protoreflect.Value) bool {
					mp[k.Interface()] = vv.Interface()
					return true
				})
				return mp, nil
			}
			if fd.IsList() {
				l := v.List()
				out := make([]any, l.Len())
				for j := 0; j < l.Len(); j++ {
					out[j] = l.Get(j).Interface()
				}
				return out, nil
			}
			return v.Interface(), nil
		}
		// Not final -> must be a message to descend
		if fd.Message() == nil {
			return nil, fmt.Errorf("cannot descend into non-message subfield %q", fname)
		}
		cur = v.Message()
		if !cur.IsValid() {
			// intermediate nil message
			return nil, nil
		}
	}
	return nil, fmt.Errorf("unreachable")
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// compareAny implements =, !=, >, <, >=, <=, :
// Notes:
// If lhs is a slice, comparisons are true if any element compares true
//
//	 For ":" on strings, lhs contains rhs is used.
//	- For "=" we attempt number/string/bool/message/reflection comparisons.
func compareAny(lhs, rhs any, op string) (bool, error) {
	// If lhs is a slice -> "any element matches" semantics.
	if isSlice(lhs) {
		sl := toSlice(lhs)
		for _, el := range sl {
			ok, err := compareAny(el, rhs, op)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}

	// If rhs is a slice -> compare against any rhs element (symmetry for lists on RHS)
	if isSlice(rhs) {
		sl := toSlice(rhs)
		for _, el := range sl {
			ok, err := compareAny(lhs, el, op)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}

	// ":" operator (has)
	if op == ":" {
		ls, lok := lhs.(string)
		rs, rok := rhs.(string)
		if lok && rok {
			return strings.Contains(ls, rs), nil
		}
		// If lhs is map, check keys/values
		if mp, ok := lhs.(map[any]any); ok {
			for k, v := range mp {
				if reflect.DeepEqual(k, rhs) || reflect.DeepEqual(v, rhs) {
					return true, nil
				}
				// string contains on string keys/values
				if ks, ok := k.(string); ok {
					if rs, rok := rhs.(string); rok && strings.Contains(ks, rs) {
						return true, nil
					}
				}
				if vs, ok := v.(string); ok {
					if rs, rok := rhs.(string); rok && strings.Contains(vs, rs) {
						return true, nil
					}
				}
			}
			return false, nil
		}
		// If lhs is non-string, fallback to equality test
		return reflect.DeepEqual(lhs, rhs), nil
	}

	// Equality / inequality
	if op == "=" || op == "!=" {
		var eq bool
		// numbers
		if ln, lok := asFloat64(lhs); lok {
			if rn, rok := asFloat64(rhs); rok {
				eq = ln == rn
			} else {
				eq = false
			}
		} else if ls, lok := lhs.(string); lok {
			if rs, rok := rhs.(string); rok {
				eq = ls == rs
			} else {
				eq = false
			}
		} else if lb, lok := lhs.(bool); lok {
			if rb, rok := rhs.(bool); rok {
				eq = lb == rb
			} else {
				eq = false
			}
		} else if lm, lok := lhs.(proto.Message); lok {
			if rm, rok := rhs.(proto.Message); rok {
				eq = proto.Equal(lm, rm)
			} else {
				eq = false
			}
		} else {
			eq = reflect.DeepEqual(lhs, rhs)
		}
		if op == "=" {
			return eq, nil
		}
		return !eq, nil
	}

	// Ordering operators: try numeric, else try string.
	if ln, lok := asFloat64(lhs); lok {
		if rn, rok := asFloat64(rhs); rok {
			switch op {
			case ">":
				return ln > rn, nil
			case "<":
				return ln < rn, nil
			case ">=":
				return ln >= rn, nil
			case "<=":
				return ln <= rn, nil
			}
		}
		return false, fmt.Errorf("rhs is not numeric for operator %q", op)
	}
	if ls, lok := lhs.(string); lok {
		if rs, rok := rhs.(string); rok {
			switch op {
			case ">":
				return ls > rs, nil
			case "<":
				return ls < rs, nil
			case ">=":
				return ls >= rs, nil
			case "<=":
				return ls <= rs, nil
			}
		}
		return false, fmt.Errorf("rhs is not string for operator %q", op)
	}

	return false, fmt.Errorf("unsupported comparator %q for types %T vs %T", op, lhs, rhs)
}

func isSlice(v any) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Slice
}

func toSlice(v any) []any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return []any{v}
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
}
