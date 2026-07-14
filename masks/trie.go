package masks

import "google.golang.org/protobuf/reflect/protoreflect"

type FieldMask struct {
	desc protoreflect.MessageDescriptor
	trie *maskTrie
}

// HasPath optimistically checks to see if a path exists in a FieldMask.
func (t *FieldMask) HasPath(path string) bool {
	if t == nil {
		return true
	}

	parts, err := tokenizePath(path)
	if err != nil {
		return false
	}

	desc := t.desc
	trie := t.trie

	return hasPath(desc, trie, parts)
}

func hasPath(desc protoreflect.MessageDescriptor, trie *maskTrie, parts []string) bool {
	part := parts[0]
	subTrie := trie.children[part]
	wildTrie := trie.children["*"]
	switch {
	case subTrie != nil:
		fd := desc.Fields().ByName(protoreflect.Name(part))
		elementTrie := subTrie
		if subTrie.children != nil {
			if star := subTrie.children["*"]; star != nil {
				elementTrie = star
			}
		}

		if fd.Kind() == protoreflect.MessageKind {
			// descend into message(s)
			if fd.IsList() {
				if len(parts) < 2 {
					return true
				}
				return hasPath(fd.Message(), elementTrie, parts[1:])
			} else if fd.IsMap() {
				// TODO: false positives are okay, optimize them away later
				return true
			} else {
				if len(parts) < 2 {
					return true
				}
				sub := fd.Message()
				return hasPath(sub, elementTrie, parts[1:])
			}
		}

	case wildTrie != nil:
		return true
	default:
		return false
	}

	return true
}
