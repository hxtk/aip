package query_test

import (
	"testing"

	"github.com/hxtk/aip/internal/testpb"
	"github.com/hxtk/aip/query"
	"github.com/tink-crypto/tink-go/v2/testing/fakekms"
)

// The fake KMS should only be used in tests. It is not secure.
const keyURI = "fake-kms://CM2b3_MDElQKSAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EhIaEIK75t5L-adlUwVhWvRuWUwYARABGM2b3_MDIAE"

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestCursorRoundtrip_Scalar(t *testing.T) {
	aead, err := fakekms.NewAEAD(keyURI)
	if err != nil {
		t.Fatalf("error creating AEAD")
	}
	aad := []byte("ctx")

	book := &testpb.Book{Title: "Dune"}

	order, err := query.ParseOrderBy("title")
	if err != nil {
		t.Fatalf("ParseOrderBy failed: %v", err)
	}

	tok, err := query.NewCursor(book, order, aead, aad)
	if err != nil {
		t.Fatalf("NewCursor failed: %v", err)
	}

	decoded, err := query.DecodeCursor[testpb.Book](tok, order, aead, aad)
	if err != nil {
		t.Fatalf("query.DecodeCursor failed: %v", err)
	}
	if decoded.GetTitle() != "Dune" {
		t.Fatalf("got %q, want %q", decoded.GetTitle(), "Dune")
	}
}

func TestCursorRoundtrip_NestedPresent(t *testing.T) {
	aead, err := fakekms.NewAEAD(keyURI)
	if err != nil {
		t.Fatalf("error creating AEAD")
	}
	aad := []byte("ctx")

	book := &testpb.Book{
		Title: "Dune",
		Author: &testpb.Author{
			GivenName:  "Frank",
			FamilyName: "Herbert",
		},
	}

	order, err := query.ParseOrderBy("author.given_name")
	if err != nil {
		t.Fatalf("ParseOrderBy failed: %v", err)
	}

	tok, err := query.NewCursor(book, order, aead, aad)
	if err != nil {
		t.Fatalf("NewCursor failed: %v", err)
	}

	decoded, err := query.DecodeCursor[testpb.Book](tok, order, aead, aad)
	if err != nil {
		t.Fatalf("query.DecodeCursor failed: %v", err)
	}

	if decoded.GetAuthor().GetGivenName() != "Frank" {
		t.Fatalf("got %q, want %q", decoded.GetAuthor().GetGivenName(), "Frank")
	}
}

func TestCursorRoundtrip_NestedMissing(t *testing.T) {
	aead, err := fakekms.NewAEAD(keyURI)
	if err != nil {
		t.Fatalf("error creating AEAD")
	}
	aad := []byte("ctx")

	book := &testpb.Book{
		Title: "Dune",
		// Author is nil
	}

	order, err := query.ParseOrderBy("author.given_name")
	if err != nil {
		t.Fatalf("ParseOrderBy failed: %v", err)
	}

	tok, err := query.NewCursor(book, order, aead, aad)
	if err != nil {
		t.Fatalf("NewCursor failed: %v", err)
	}

	decoded, err := query.DecodeCursor[testpb.Book](tok, order, aead, aad)
	if err != nil {
		t.Fatalf("query.DecodeCursor failed: %v", err)
	}

	// Should treat missing author as zero-value, so given_name == ""
	if decoded.GetAuthor().GetGivenName() != "" {
		t.Fatalf("expected empty given_name for missing author")
	}
}

func TestDecodeWithDifferentOrderFails(t *testing.T) {
	aead, err := fakekms.NewAEAD(keyURI)
	if err != nil {
		t.Fatalf("error creating AEAD")
	}
	aad := []byte("ctx")

	book := &testpb.Book{Title: "Dune"}
	orderAsc, _ := query.ParseOrderBy("title")
	orderDesc, _ := query.ParseOrderBy("title desc")

	tok, err := query.NewCursor(book, orderAsc, aead, aad)
	if err != nil {
		t.Fatalf("NewCursor failed: %v", err)
	}

	if _, err := query.DecodeCursor[testpb.Book](tok, orderDesc, aead, aad); err == nil {
		t.Fatalf("DecodeCursor succeeded with different order; expected failure")
	}
}

func TestTamperedTokenFails(t *testing.T) {
	aead, err := fakekms.NewAEAD(keyURI)
	if err != nil {
		t.Fatalf("error creating AEAD")
	}
	aad := []byte("ctx")
	book := &testpb.Book{Title: "Dune"}
	order, _ := query.ParseOrderBy("title")

	tok, err := query.NewCursor(book, order, aead, aad)
	if err != nil {
		t.Fatalf("NewCursor failed: %v", err)
	}

	b := []byte(tok)
	b[len(b)-1] ^= 1 // flip a bit
	tampered := string(b)

	if _, err := query.DecodeCursor[testpb.Book](tampered, order, aead, aad); err == nil {
		t.Fatalf("DecodeCursor succeeded with tampered token; expected failure")
	}
}

func TestLessAndCursorFilter(t *testing.T) {
	orderAsc, err := query.ParseOrderBy("title")
	if err != nil {
		t.Fatalf("ParseOrderBy failed: %v", err)
	}

	a := &testpb.Book{Title: "A"}
	cursor := &testpb.Book{Title: "B"}
	filter, err := query.CursorFilter(cursor, orderAsc)
	if err != nil {
		t.Fatalf("CursorFilter failed: %v", err)
	}

	c := &testpb.Book{Title: "C"}
	if !filter(c) {
		t.Errorf("expected C > B")
	}
	if filter(a) {
		t.Errorf("expected A <= B")
	}
}

func TestRejectMapFieldsInSortKeys(t *testing.T) {
	// Test string-keyed map
	order, err := query.ParseOrderBy("reviews")
	if err != nil {
		t.Logf("ParseOrderBy('reviews') rejected: %v", err)
	}

	_, err = query.CursorFilter(&testpb.Book{
		Reviews: map[string]string{
			"test review": "I liked the book okay",
		},
	}, order)
	if err == nil {
		t.Fatalf("expected error for map field 'reviews'")
	}
}

