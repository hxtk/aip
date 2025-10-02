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

package aip

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/hxtk/aip/internal/testpb"
	. "github.com/hxtk/aip/query/internal/assertions"
)

func TestOrderByClause(t *testing.T) {
	Convey("OrderByClause", t, func() {
		table := NewTable().WithColumns(
			NewColumn().WithFieldPath("foo").WithDatabaseName("db_foo").Sortable().Build(),
			NewColumn().WithFieldPath("bar").WithDatabaseName("db_bar").Sortable().Build(),
			NewColumn().WithFieldPath("baz").WithDatabaseName("db_baz").Sortable().Build(),
			NewColumn().WithFieldPath("unsortable").WithDatabaseName("unsortable").Build(),
		).Build()

		Convey("Empty order by", func() {
			result, err := table.OrderByClause([]OrderBy{})
			So(err, ShouldBeNil)
			So(result, ShouldEqual, "")
		})
		Convey("Single order by", func() {
			result, err := table.OrderByClause([]OrderBy{
				{
					FieldPath: NewFieldPath("foo"),
				},
			})
			So(err, ShouldBeNil)
			So(result, ShouldEqual, "ORDER BY db_foo\n")
		})
		Convey("Multiple order by", func() {
			result, err := table.OrderByClause([]OrderBy{
				{
					FieldPath:  NewFieldPath("foo"),
					Descending: true,
				},
				{
					FieldPath: NewFieldPath("bar"),
				},
				{
					FieldPath:  NewFieldPath("baz"),
					Descending: true,
				},
			})
			So(err, ShouldBeNil)
			So(result, ShouldEqual, "ORDER BY db_foo DESC, db_bar, db_baz DESC\n")
		})
		Convey("Unsortable field in order by", func() {
			_, err := table.OrderByClause([]OrderBy{
				{
					FieldPath:  NewFieldPath("unsortable"),
					Descending: true,
				},
			})
			So(err, ShouldErrLike, `no sortable field named "unsortable", valid fields are foo, bar, baz`)
		})
		Convey("Repeated field in order by", func() {
			_, err := table.OrderByClause([]OrderBy{
				{
					FieldPath: NewFieldPath("foo"),
				},
				{
					FieldPath: NewFieldPath("foo"),
				},
			})
			So(err, ShouldErrLike, `field appears in order_by multiple times: "foo"`)
		})
	})
}

func TestMergeWithDefaultOrder(t *testing.T) {
	Convey("MergeWithDefaultOrder", t, func() {
		defaultOrder := []OrderBy{
			{
				FieldPath:  NewFieldPath("foo"),
				Descending: true,
			}, {
				FieldPath: NewFieldPath("bar"),
			}, {
				FieldPath:  NewFieldPath("baz"),
				Descending: true,
			},
		}
		Convey("Empty order", func() {
			result := MergeWithDefaultOrder(defaultOrder, nil)
			So(result, ShouldResemble, defaultOrder)
		})
		Convey("Non-empty order", func() {
			order := []OrderBy{
				{
					FieldPath:  NewFieldPath("other"),
					Descending: true,
				},
				{
					FieldPath: NewFieldPath("baz"),
				},
			}
			result := MergeWithDefaultOrder(defaultOrder, order)
			So(result, ShouldResemble, []OrderBy{
				{
					FieldPath:  NewFieldPath("other"),
					Descending: true,
				},
				{
					FieldPath: NewFieldPath("baz"),
				},
				{
					FieldPath:  NewFieldPath("foo"),
					Descending: true,
				}, {
					FieldPath: NewFieldPath("bar"),
				},
			})
		})
	})
}

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
