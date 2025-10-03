package query

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/tink-crypto/tink-go/v2/tink"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	ErrInvalidPageToken = errors.New("invalid page token")
	ErrInvalidOrder     = errors.New("invalid order for message type")
)

// DecodeCursor parses a Page Token string
func DecodeCursor[S any, M interface {
	proto.Message
	*S
}](token string, order []OrderBy, aead tink.AEAD, aad []byte) (M, error) {
	cipher, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}

	aad = slices.Concat(aad, []byte{0}, serializeOrderByText(order))
	data, err := aead.Decrypt(cipher, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}

	var zero S
	var msg M = &zero

	err = proto.Unmarshal(data, msg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}

	return msg, nil
}

// CursorFilter generates a filter from a proto.Message and an iteration order.
//
// If the order validates for the message type, it returns a function
// that accept a proto.Message value of the same type and returns true
// if the parameter comes after the cursor message in the sort order.
func CursorFilter[M proto.Message](cursor M, order []OrderBy) (func(M) bool, error) {
	less, err := Less[M](order)
	if err != nil {
		return nil, err
	}

	return func(msg M) bool {
		return less(cursor, msg)
	}, nil
}

func NewCursor(m proto.Message, order []OrderBy, aead tink.AEAD, aad []byte) (string, error) {
	pruned, err := pruneMessage(m, order)
	if err != nil {
		return "", fmt.Errorf("pruning message: %w", err)
	}

	raw, err := proto.Marshal(pruned)
	if err != nil {
		return "", fmt.Errorf("marshaling pruned message: %w", err)
	}

	aad = slices.Concat(aad, []byte{0}, serializeOrderByText(order))
	ciphertext, err := aead.Encrypt(raw, aad)
	if err != nil {
		return "", fmt.Errorf("encrypting token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

func serializeOrderByText(order []OrderBy) []byte {
	parts := make([]string, len(order))
	for i, ob := range order {
		dir := "asc"
		if ob.Descending {
			dir = "desc"
		}
		parts[i] = ob.FieldPath.canonical + ":" + dir
	}
	return []byte(strings.Join(parts, "|"))
}

// pruneMessage returns a new proto.Message containing only the fields needed for the sort order.
func pruneMessage(msg proto.Message, orderBy []OrderBy) (proto.Message, error) {
	m := msg.ProtoReflect()
	pruned := m.New().Interface()

	for _, ob := range orderBy {
		if err := copyFieldPath(m, pruned.ProtoReflect(), ob.FieldPath.segments); err != nil {
			return nil, fmt.Errorf("copying field %s: %w", ob.FieldPath.canonical, err)
		}
	}

	return pruned, nil
}

// copyFieldPath recursively copies a nested field from src to dst based on segments.
func copyFieldPath(src, dst protoreflect.Message, segments []string) error {
	if len(segments) == 0 {
		return nil
	}

	fieldName := protoreflect.Name(segments[0])
	fieldDesc := src.Descriptor().Fields().ByName(fieldName)
	if fieldDesc == nil {
		return fmt.Errorf("field %s not found in message %s", fieldName, src.Descriptor().FullName())
	}
	if fieldDesc.Cardinality() == protoreflect.Repeated {
		return fmt.Errorf("cannot sort on repeated field %s in message %s", segments[0], fieldDesc.FullName())
	}

	val := src.Get(fieldDesc)
	if len(segments) == 1 {
		dst.Set(fieldDesc, val)
		return nil
	}

	if fieldDesc.Message() == nil {
		return fmt.Errorf("field %s is not a message, cannot descend", fieldName)
	}

	var childDst protoreflect.Message
	if dst.Has(fieldDesc) {
		childDst = dst.Mutable(fieldDesc).Message()
	} else {
		childDst = dst.NewField(fieldDesc).Message()
		dst.Set(fieldDesc, protoreflect.ValueOfMessage(childDst))
	}

	return copyFieldPath(val.Message(), childDst, segments[1:])
}
