package masks

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func WithReadMaskInterceptor(header string) connect.Interceptor {
	return &connectInterceptor{header: header}
}

type connectInterceptor struct {
	header string
}

// WrapStreamingClient implements connect.Interceptor.
func (c *connectInterceptor) WrapStreamingClient(fn connect.StreamingClientFunc) connect.StreamingClientFunc {
	return fn
}

// WrapStreamingHandler implements connect.Interceptor.
func (c *connectInterceptor) WrapStreamingHandler(fn connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, h connect.StreamingHandlerConn) error {
		meth, ok := h.Spec().Schema.(protoreflect.MethodDescriptor)
		if !ok {
			return fn(ctx, h)
		}

		headerVal := h.RequestHeader().Get(c.header)
		if headerVal == "" {
			return fn(ctx, h)
		}
		fields := splitComma(headerVal)

		mask, err := New(meth.Output(), ModeRead, fields...)
		if err != nil {
			return connect.NewError(
				connect.CodeInvalidArgument,
				err,
			)
		}

		return fn(ctx, &pruningConn{
			StreamingHandlerConn: h,
			fm:                   mask,
		})
	}

}

type pruningConn struct {
	connect.StreamingHandlerConn
	fm *fieldmaskpb.FieldMask
}

func (c *pruningConn) Send(msg any) error {
	pm, ok := msg.(proto.Message)
	if !ok {
		return c.StreamingHandlerConn.Send(msg)
	}

	err := PruneMessage(pm, c.fm)
	if err != nil {
		return err
	}

	return c.StreamingHandlerConn.Send(msg)
}

// WrapUnary implements connect.Interceptor.
func (c *connectInterceptor) WrapUnary(fn connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		meth, ok := req.Spec().Schema.(protoreflect.MethodDescriptor)
		if !ok {
			return fn(ctx, req)
		}

		headerVal := req.Header().Get(c.header)
		if headerVal == "" {
			return fn(ctx, req)
		}
		fields := splitComma(headerVal)

		mask, err := New(meth.Output(), ModeRead, fields...)
		if err != nil {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				err,
			)
		}

		rsp, err := fn(ctx, req)
		if err != nil {
			return nil, err
		}

		pm, ok := rsp.Any().(proto.Message)
		if !ok {
			return rsp, nil
		}

		err = PruneMessage(pm, mask)
		if err != nil {
			return nil, connect.NewError(
				connect.CodeInternal,
				err,
			)
		}

		return rsp, nil
	}
}

var _ connect.Interceptor = (*connectInterceptor)(nil)

// splitComma parses a comma-separated list into fields, trimming whitespace.
func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
