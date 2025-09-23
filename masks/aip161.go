package masks

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Mode represents whether the mask is being used for a read or write.
type Mode int

const (
	ModeRead Mode = iota
	ModeWrite
)

// New validates the given paths against the descriptor according to AIP-161
// and returns a normalized FieldMask if valid.
func New(desc protoreflect.MessageDescriptor, mode Mode, paths ...string) (*fieldmaskpb.FieldMask, error) {
	var validPaths []string
	for _, p := range paths {
		if err := validatePath(desc, mode, p); err != nil {
			return nil, fmt.Errorf("invalid field mask path %q: %w", p, err)
		}
		validPaths = append(validPaths, p)
	}
	return &fieldmaskpb.FieldMask{Paths: validPaths}, nil
}

// --- path validation ---

func validatePath(desc protoreflect.MessageDescriptor, mode Mode, path string) error {
	if path == "" {
		return fmt.Errorf("empty path")
	}
	segments, err := tokenizePath(path)
	if err != nil {
		return err
	}

	curr := desc
	for i, seg := range segments {
		isLast := i == len(segments)-1

		switch {
		case seg == "*":
			// Wildcard must appear on repeated or map fields, not on scalars.
			if i == 0 {
				return fmt.Errorf("wildcard cannot be top-level")
			}
			// We already validated that the previous segment was a repeated/map field.
			continue

		case strings.HasPrefix(seg, "`") && strings.HasSuffix(seg, "`"):
			// Backtick-quoted map key
			key := seg[1 : len(seg)-1]
			if strings.ContainsRune(key, '`') {
				return fmt.Errorf("malformed backtick quoting in %q", seg)
			}
			// Must follow a map field
			if i == 0 {
				return fmt.Errorf("map key %q without map field", seg)
			}
			continue

		default:
			// Regular identifier or numeric literal
			f := curr.Fields().ByName(protoreflect.Name(seg))
			if f == nil {
				// Not a field — check if it could be a map key
				if isAllDigits(seg) {
					// numeric token
					if i == 0 {
						return fmt.Errorf("numeric token %q cannot be top-level", seg)
					}
					// Validate parent field is a map with int key
					parentField := findFieldBySegment(curr, segments[i-1])
					if parentField == nil {
						return fmt.Errorf("numeric key %q without parent field", seg)
					}
					if parentField.IsMap() && isIntegerKind(parentField.MapKey().Kind()) {
						continue
					}
					return fmt.Errorf("numeric index %q not allowed", seg)
				}
				// If nonexistent field
				if mode == ModeWrite {
					return fmt.Errorf("field %q does not exist", seg)
				}
				// ModeRead: tolerate nonexistent field by stopping traversal.
				return nil
			}

			// Found a real field
			if f.IsList() || f.IsMap() {
				// ok; subfield allowed (wildcard, map key, or subfield traversal)
				if !isLast {
					// must be followed by * or key or subfield
					// we’ll check at next iteration
				}
			} else if f.Kind() == protoreflect.MessageKind {
				// Embedded message — descend into it
				curr = f.Message()
			} else {
				// Scalar field
				if !isLast {
					return fmt.Errorf("cannot traverse into scalar field %q", seg)
				}
			}
		}
	}

	return nil
}

// tokenizePath splits a field mask path into segments,
// handling backtick-quoted keys.
func tokenizePath(path string) ([]string, error) {
	var segs []string
	var b strings.Builder
	inQuote := false

	for i, r := range path {
		switch r {
		case '.':
			if !inQuote {
				if b.Len() == 0 {
					return nil, fmt.Errorf("empty segment at %d", i)
				}
				segs = append(segs, b.String())
				b.Reset()
				continue
			}
		case '`':
			inQuote = !inQuote
		}
		b.WriteRune(r)
	}

	if inQuote {
		return nil, fmt.Errorf("unclosed backtick")
	}
	if b.Len() > 0 {
		segs = append(segs, b.String())
	}
	return segs, nil
}

// --- helpers ---

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isIntegerKind(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
		return true
	}
	return false
}

func findFieldBySegment(desc protoreflect.MessageDescriptor, seg string) protoreflect.FieldDescriptor {
	return desc.Fields().ByName(protoreflect.Name(seg))
}
