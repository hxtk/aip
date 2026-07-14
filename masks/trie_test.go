package masks_test

import (
	"testing"

	"github.com/hxtk/aip/internal/testpb"
	"github.com/hxtk/aip/masks"
)

func TestHasPath(t *testing.T) {
	type testCase struct {
		name  string
		paths []string
		path  string
		want  bool
	}

	desc := new(testpb.Book).ProtoReflect().Descriptor()
	tcs := []testCase{}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mask, err := masks.New(desc, masks.ModeRead, tc.paths...)
			if err != nil {
				t.Fatalf("masks.New(desc, masks.ModeRead, %v...) = _, %v.", tc.paths, err)
			}

			got := mask.HasPath(tc.path)
			if tc.want != got {
			}
		})
	}
}
