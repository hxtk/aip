package query_test

import (
	"testing"

	"github.com/hxtk/aip/internal/testpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	aip "github.com/hxtk/aip/query"
)

func TestMatchesFilter_Basic(t *testing.T) {
	book := &testpb.Book{
		Title: "The Pragmatic Programmer",
		Author: &testpb.Author{
			GivenName:  "Andy",
			FamilyName: "Hunt",
		},
		Name: "books/123",
	}

	tests := []struct {
		name     string
		filter   string
		expected bool
	}{
		// Equality
		{"string equality (match)", `title = "The Pragmatic Programmer"`, true},
		{"string equality (no match)", `title = "Clean Code"`, false},

		// Inequality
		{"string inequality (match)", `title != "Clean Code"`, true},
		{"string inequality (no match)", `title != "The Pragmatic Programmer"`, false},

		// Nested field
		{"nested field equality", `author.family_name = "Hunt"`, true},
		{"nested field inequality", `author.family_name = "Martin"`, false},

		// Negation
		{"negated equality", `NOT title = "Clean Code"`, true},
		{"negated match fails", `- title = "The Pragmatic Programmer"`, false},

		// Logical AND / OR
		{"AND (both true)", `title = "The Pragmatic Programmer" AND author.family_name = "Hunt"`, true},
		{"AND (one false)", `title = "The Pragmatic Programmer" AND author.family_name = "Martin"`, false},
		{"OR (one true)", `title = "The Pragmatic Programmer" OR author.family_name = "Martin"`, true},
		{"OR (both false)", `title = "Clean Code" OR author.family_name = "Martin"`, false},

		// "has" operator
		{"has substring (match)", `title : "Pragmatic"`, true},
		{"has substring (no match)", `title : "Refactoring"`, false},

		// Parentheses
		{"grouped expression", `(title = "Clean Code" OR title = "The Pragmatic Programmer") AND author.family_name = "Hunt"`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := aip.ParseFilter(tc.filter)
			require.NoError(t, err, "parse filter")

			filter, err := aip.ProtoFilter[testpb.Book](f)
			require.NoError(t, err, "evaluate filter")

			ok := filter(proto.Clone(book).(*testpb.Book))
			require.Equal(t, tc.expected, ok)
		})
	}
}

func TestMatchesFilter_GlobalRestriction(t *testing.T) {
	book := &testpb.Book{
		Title: "The Pragmatic Programmer",
		Author: &testpb.Author{
			GivenName:  "Andy",
			FamilyName: "Hunt",
		},
		Authors: []*testpb.Author{
			{GivenName: "Andy", FamilyName: "Hunt"},
			{GivenName: "Dave", FamilyName: "Thomas"},
		},
		Reviews: map[string]string{
			"review1": "Classic software engineering advice",
			"review2": "By Andy Hunt and Dave Thomas",
		},
		Items: map[int32]string{
			1: "hardcover",
			2: "ebook",
		},
		Name: "books/123",
	}

	tests := []struct {
		name     string
		filter   string
		expected bool
	}{
		// --- Top-level field matches ---
		{"matches title", "Pragmatic", true},
		{"matches author family name", "Hunt", true},
		{"matches author given name", "Andy", true},

		// --- Nested and repeated fields ---
		{"matches repeated author", "Thomas", true},
		{"matches nested structure case-insensitive", "prAgMaTic", true},

		// --- Map fields ---
		{"matches map value", "classic", true},
		{"matches map value in lowercase", "advice", true},
		{"matches map value across keys", "review1", true}, // key matches
		{"matches map key substring", "review", true},

		// --- Non-matching term ---
		{"does not match non-existent term", "Refactoring", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := aip.ParseFilter(tc.filter)
			require.NoError(t, err, "parse filter")

			filter, err := aip.ProtoFilter[testpb.Book](f)
			require.NoError(t, err, "evaluate filter")

			ok := filter(proto.Clone(book).(*testpb.Book))
			require.Equal(t, tc.expected, ok)
		})
	}
}

func TestMatchesFilter_GlobalAndExplicit(t *testing.T) {
	book := &testpb.Book{
		Title: "The Pragmatic Programmer",
		Author: &testpb.Author{
			GivenName:  "Andy",
			FamilyName: "Hunt",
		},
		Authors: []*testpb.Author{
			{GivenName: "Andy", FamilyName: "Hunt"},
			{GivenName: "Dave", FamilyName: "Thomas"},
		},
		Reviews: map[string]string{
			"review1": "Classic software engineering advice",
			"review2": "By Andy Hunt and Dave Thomas",
		},
		Items: map[int32]string{
			1: "hardcover",
			2: "ebook",
		},
		Name: "books/123",
	}

	tests := []struct {
		name     string
		filter   string
		expected bool
	}{
		// Global + explicit AND
		{
			name:     `global matches + explicit matches`,
			filter:   `Pragmatic AND author.family_name = "Hunt"`,
			expected: true,
		},
		{
			name:     `global matches + explicit does not match`,
			filter:   `Pragmatic AND author.family_name = "Martin"`,
			expected: false,
		},
		{
			name:     `global does not match + explicit matches`,
			filter:   `Refactoring AND author.family_name = "Hunt"`,
			expected: false,
		},

		// Global + explicit OR
		{
			name:     `global matches OR explicit false`,
			filter:   `Pragmatic OR author.family_name = "Martin"`,
			expected: true,
		},
		{
			name:     `global does not match OR explicit matches`,
			filter:   `Refactoring OR author.family_name = "Hunt"`,
			expected: true,
		},
		{
			name:     `neither global nor explicit match`,
			filter:   `Refactoring OR author.family_name = "Martin"`,
			expected: false,
		},

		// Global nested with parentheses
		{
			name:     `parentheses with AND/OR`,
			filter:   `(Refactoring OR Pragmatic) AND author.family_name = "Hunt"`,
			expected: true,
		},
		{
			name:     `parentheses suppress global hit`,
			filter:   `(Refactoring OR Nonexistent) AND author.family_name = "Hunt"`,
			expected: false,
		},

		// Negation
		{
			name:     `NOT global true`,
			filter:   `NOT Pragmatic`,
			expected: false,
		},
		{
			name:     `NOT global false`,
			filter:   `NOT Refactoring`,
			expected: true,
		},
		{
			name:     `negated composite`,
			filter:   `NOT (Pragmatic OR Refactoring)`,
			expected: false,
		},
		{
			name:     `double condition with negation`,
			filter:   `NOT Refactoring AND author.family_name = "Hunt"`,
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := aip.ParseFilter(tc.filter)
			require.NoError(t, err, "parse filter")

			filter, err := aip.ProtoFilter[testpb.Book](f)
			require.NoError(t, err, "evaluate filter")

			ok := filter(proto.Clone(book).(*testpb.Book))
			require.Equal(t, tc.expected, ok)
		})
	}
}
