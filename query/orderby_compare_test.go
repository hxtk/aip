package query

import (
	"testing"

	"github.com/hxtk/aip/internal/testpb"
)

func TestLess(t *testing.T) {
	tests := []struct {
		name  string
		order string
		a, b  *testpb.Book
		want  bool
	}{
		{
			name:  "sort by title asc",
			order: "title",
			a:     &testpb.Book{Title: "A Tale of Two Cities"},
			b:     &testpb.Book{Title: "War and Peace"},
			want:  true, // "A" < "W"
		},
		{
			name:  "sort by title desc",
			order: "title desc",
			a:     &testpb.Book{Title: "A Tale of Two Cities"},
			b:     &testpb.Book{Title: "War and Peace"},
			want:  false, // descending: "A" comes after "W"
		},
		{
			name:  "sort by author.given_name asc, tie on family_name",
			order: "author.given_name, author.family_name",
			a: &testpb.Book{
				Author: &testpb.Author{GivenName: "Alice", FamilyName: "Zebra"},
			},
			b: &testpb.Book{
				Author: &testpb.Author{GivenName: "Alice", FamilyName: "Anderson"},
			},
			want: false, // tie on given_name, but Zebra > Anderson
		},
		{
			name:  "sort by author.family_name desc",
			order: "author.family_name desc",
			a: &testpb.Book{
				Author: &testpb.Author{FamilyName: "Taylor"},
			},
			b: &testpb.Book{
				Author: &testpb.Author{FamilyName: "Smith"},
			},
			want: true, // descending: "Smith" > "Taylor"
		},
		{
			name:  "missing author treated as empty",
			order: "author.given_name",
			a:     &testpb.Book{Title: "No Author"},
			b:     &testpb.Book{Author: &testpb.Author{GivenName: "Bob"}},
			want:  true, // "" < "Bob"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := ParseOrderBy(tt.order)
			if err != nil {
				t.Fatalf("Parsing filter failed: %v", err)
			}

			less, err := Less[*testpb.Book](order)
			if err != nil {
				t.Fatalf("Less constructor failed: %v", err)
			}

			got := less(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Less(%v,%v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestRejectRepeatedFieldInSortKeys(t *testing.T) {
	order, err := ParseOrderBy("authors")
	if err == nil {
		t.Errorf("ParseOrderBy('authors') rejected: %v", err)
	}

	_, lessErr := Less[*testpb.Book](order)
	if lessErr == nil {
		t.Fatalf("expected error for repeated field in sort keys")
	}
}

func TestNonexistentFieldInSortKeys(t *testing.T) {
	order, err := ParseOrderBy("no_such_field")
	if err == nil {
		_, lessErr := Less[*testpb.Book](order)
		if lessErr == nil {
			t.Fatalf("expected error for nonexistent field")
		}
	} else {
		t.Logf("ParseOrderBy('no_such_field') rejected: %v", err)
	}
}
