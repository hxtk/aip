// file: aip161mask_test.go
package masks_test

import (
	"testing"

	masks "github.com/hxtk/aip/masks"
	"github.com/hxtk/aip/masks/internal/testpb"
)

func TestNew_AIP161Semantics(t *testing.T) {
	desc := new(testpb.Book).ProtoReflect().Descriptor()

	cases := []struct {
		name    string
		paths   []string
		mode    masks.Mode
		wantErr bool
	}{
		{"simple field", []string{"title"}, masks.ModeRead, false},
		{"nested field", []string{"author.given_name"}, masks.ModeRead, false},
		{"map key simple", []string{"reviews.smith"}, masks.ModeRead, false},
		{"map key backtick", []string{"reviews.`John Smith`"}, masks.ModeRead, false},
		{"map int key", []string{"items.123"}, masks.ModeRead, false},
		{"wildcard repeated", []string{"authors.*.given_name"}, masks.ModeRead, false},
		{"wildcard on scalar invalid", []string{"title.*"}, masks.ModeRead, true},
		{"numeric index invalid", []string{"authors.0.given_name"}, masks.ModeRead, true},
		{"nonexistent field read tolerated", []string{"does_not_exist"}, masks.ModeRead, false},
		{"nonexistent field write invalid", []string{"does_not_exist"}, masks.ModeWrite, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := masks.New(desc, tc.mode, tc.paths...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}
