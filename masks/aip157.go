package masks

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// PruneMessage traverses msg and clears fields that are not present in mask.
// The mask must be valid under ModeRead for msg’s descriptor.
func PruneMessage(msg proto.Message, mask *fieldmaskpb.FieldMask) error {
	if msg == nil {
		return nil
	}
	if mask == nil {
		return nil
	}
	// Build a trie of mask paths for fast lookup during traversal.
	trie := newMaskTrie(mask.Paths)
	return pruneMessage(msg.ProtoReflect(), trie)
}

// pruneMessage applies pruning recursively.
func pruneMessage(m protoreflect.Message, trie *maskTrie) error {
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())

		subTrie := trie.children[name]
		wildTrie := trie.children["*"]

		switch {
		case subTrie != nil:
			// Field explicitly present in mask.
			// Determine which trie should be used for the *element* or value:
			// if subTrie contains a "*" child, that wildcard is consumed when
			// descending into elements/values.
			elementTrie := subTrie
			if subTrie.children != nil {
				if star := subTrie.children["*"]; star != nil {
					elementTrie = star
				}
			}

			if fd.Kind() == protoreflect.MessageKind {
				// descend into message(s)
				if fd.IsList() {
					list := m.Mutable(fd).List()
					for idx := 0; idx < list.Len(); idx++ {
						pm := list.Get(idx).Message()
						if pm.IsValid() {
							if err := pruneMessage(pm, elementTrie); err != nil {
								return err
							}
						}
					}
				} else if fd.IsMap() {
					mapVal := m.Mutable(fd).Map()
					for _, val := range mapVal.Range {
						if fd.MapValue().Kind() == protoreflect.MessageKind {
							pm := val.Message()
							if pm.IsValid() {
								if err := pruneMessage(pm, elementTrie); err != nil {
									return err
								}
							}
						}
					}
				} else {
					sub := m.Mutable(fd).Message()
					if sub.IsValid() {
						if err := pruneMessage(sub, elementTrie); err != nil {
							return err
						}
					}
				}
			}
			// scalar fields are kept as-is when explicitly listed

		case wildTrie != nil:
			// Wildcard present at THIS level of the trie:
			// apply wildcard semantics (for messages we descend into each element/value
			// using the wildcard's child trie).
			// The wildcard node itself can have children (e.g. `authors.*.given_name`),
			// so we should descend using wildTrie (which already corresponds to the '*'
			// node's children).
			if fd.IsList() && fd.Kind() == protoreflect.MessageKind {
				list := m.Mutable(fd).List()
				for idx := 0; idx < list.Len(); idx++ {
					pm := list.Get(idx).Message()
					if pm.IsValid() {
						if err := pruneMessage(pm, wildTrie); err != nil {
							return err
						}
					}
				}
			}
			if fd.IsMap() && fd.MapValue().Kind() == protoreflect.MessageKind {
				mapVal := m.Mutable(fd).Map()
				for _, val := range mapVal.Range {
					pm := val.Message()
					if pm.IsValid() {
						if err := pruneMessage(pm, wildTrie); err != nil {
							return err
						}
					}
				}
			}
			// For scalar lists/maps/scalars: wildcard keeps them entirely.

		default:
			// Not in mask at this level -> clear whole field
			m.Clear(fd)
		}
	}
	return nil
}

// --- trie of mask paths ---

type maskTrie struct {
	children map[string]*maskTrie
}

func newMaskTrie(paths []string) *maskTrie {
	root := &maskTrie{children: map[string]*maskTrie{}}
	for _, p := range paths {
		segments, _ := tokenizePath(p) // reuse tokenizer from aip161mask.go
		curr := root
		for _, s := range segments {
			if curr.children == nil {
				curr.children = map[string]*maskTrie{}
			}
			child := curr.children[s]
			if child == nil {
				child = &maskTrie{children: map[string]*maskTrie{}}
				curr.children[s] = child
			}
			curr = child
		}
	}
	return root
}
