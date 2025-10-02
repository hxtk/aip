package masks_test

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/hxtk/aip/internal/testpb"
	"github.com/hxtk/aip/masks"
)

// Book and Author types should come from generated protos in real usage.
// For unit tests here we can define dummy structs implementing proto.Message
// or reuse the descriptor-building helpers with dynamic messages.
// To keep tests simple, we’ll assume generated types exist as testpb.Book / testpb.Author.

func TestPruneMessage_BasicScalars(t *testing.T) {
	book := &testpb.Book{
		Title: "keep me",
		Name:  "drop me",
	}

	mask := &fieldmaskpb.FieldMask{Paths: []string{"title"}}

	if err := masks.PruneMessage(book, mask); err != nil {
		t.Fatal(err)
	}

	if book.Title == "" {
		t.Errorf("expected Title to be kept")
	}
	if book.Name != "" {
		t.Errorf("expected Name to be cleared, got %q", book.Name)
	}
}

func TestPruneMessage_NestedMessage(t *testing.T) {
	book := &testpb.Book{
		Author: &testpb.Author{
			GivenName:  "keep",
			FamilyName: "drop",
		},
	}

	mask := &fieldmaskpb.FieldMask{Paths: []string{"author.given_name"}}

	if err := masks.PruneMessage(book, mask); err != nil {
		t.Fatal(err)
	}

	if got := book.Author.GivenName; got == "" {
		t.Errorf("expected GivenName to be kept")
	}
	if got := book.Author.FamilyName; got != "" {
		t.Errorf("expected FamilyName cleared, got %q", got)
	}
}

func TestPruneMessage_RepeatedMessage(t *testing.T) {
	book := &testpb.Book{
		Authors: []*testpb.Author{
			{GivenName: "keep1", FamilyName: "drop1"},
			{GivenName: "keep2", FamilyName: "drop2"},
		},
	}

	mask := &fieldmaskpb.FieldMask{Paths: []string{"authors.given_name"}}

	if err := masks.PruneMessage(book, mask); err != nil {
		t.Fatal(err)
	}

	for i, a := range book.Authors {
		if a.GivenName == "" {
			t.Errorf("authors[%d].GivenName should be kept", i)
		}
		if a.FamilyName != "" {
			t.Errorf("authors[%d].FamilyName should be cleared, got %q", i, a.FamilyName)
		}
	}
}

func TestPruneMessage_WildcardRepeatedMessage(t *testing.T) {
	book := &testpb.Book{
		Authors: []*testpb.Author{
			{GivenName: "keep1", FamilyName: "drop1"},
			{GivenName: "keep2", FamilyName: "drop2"},
		},
	}

	mask := &fieldmaskpb.FieldMask{Paths: []string{"authors.*.given_name"}}

	if err := masks.PruneMessage(book, mask); err != nil {
		t.Fatal(err)
	}

	for i, a := range book.Authors {
		if a.GivenName == "" {
			t.Errorf("authors[%d].GivenName should be kept", i)
		}
		if a.FamilyName != "" {
			t.Errorf("authors[%d].FamilyName should be cleared, got %q", i, a.FamilyName)
		}
	}
}

func TestPruneMessage_ClearsUnmentionedFields(t *testing.T) {
	book := &testpb.Book{
		Title: "keep",
		Author: &testpb.Author{
			GivenName: "drop",
		},
		Reviews: map[string]string{"smith": "drop"},
	}

	mask := &fieldmaskpb.FieldMask{Paths: []string{"title"}}

	if err := masks.PruneMessage(book, mask); err != nil {
		t.Fatal(err)
	}

	want := &testpb.Book{Title: "keep"}
	if !proto.Equal(book, want) {
		t.Errorf("got %v, want %v", book, want)
	}
}

func TestPruneMessage_NilMaskNoop(t *testing.T) {
	orig := &testpb.Book{Title: "keep", Name: "keep"}
	book := proto.Clone(orig).(*testpb.Book)

	if err := masks.PruneMessage(book, nil); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(book, orig) {
		t.Errorf("expected no-op when mask=nil, got %v", book)
	}
}
