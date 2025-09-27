package masks

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func PruneMessage(proto.Message, *fieldmaskpb.FieldMask) error {
	return nil
}
