// file: aip161mask_test.go
package masks_test

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	masks "github.com/hxtk/aip/masks"
)

func buildTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	// Build a synthetic descriptor for tests.
	// Equivalent .proto would be:
	//
	// message Author {
	//   string given_name = 1;
	//   string family_name = 2;
	// }
	// message Book {
	//   string title = 1;
	//   Author author = 2;
	//   repeated Author authors = 3;
	//   map<string, string> reviews = 4;
	//   map<int32, string> items = 5;
	//   string name = 6; // output-only
	// }
	//
	// (In a real implementation you’d use generated descriptors.)
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("book.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Author"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("given_name"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
					{Name: proto.String("family_name"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				},
			},
			{
				Name: proto.String("Book"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("title"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
					{Name: proto.String("author"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), TypeName: proto.String("Author")},
					{Name: proto.String("authors"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), TypeName: proto.String("Author")},
					{Name: proto.String("reviews"), Number: proto.Int32(4), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String("ReviewsEntry")},
					{Name: proto.String("items"), Number: proto.Int32(5), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String("ItemsEntry")},
					{Name: proto.String("name"), Number: proto.Int32(6), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("ReviewsEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
							{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
					{
						Name: proto.String("ItemsEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()},
							{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
		},
	}
	file, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return file.Messages().ByName("Book")
}

func TestNew_AIP161Semantics(t *testing.T) {
	desc := buildTestDescriptor(t)

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
