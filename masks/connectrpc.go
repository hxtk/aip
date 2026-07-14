package masks

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type masksCtxKey struct{}

var ctxKey = masksCtxKey{}

// HasPath checks whether a given field path exists in the field mask in ctx.
//
// If the provided context has no mask embedded or has a nil mask embedded,
// this method unconditionally returns true.
//
// If a non-trivial mask exists in the context, if this method returns false,
// that implies the mask would not prune the queried path. Any instance where
// this function returns true for a field that the mask would prune is a bug,
// but for correctness sake, this implementation errs on the side of inclusiveness.
//
// If you're implementing an RPC, you may use this method to decide whether
// your handler needs to do work to produce a non-trivial field on the response.
func HasPath(ctx context.Context, path string) bool {
	mask, ok := ctx.Value(ctxKey).(*FieldMask)
	if !ok {
		return true
	}

	return mask.HasPath(path)
}

func MaskContext(ctx context.Context, mask *FieldMask) context.Context {
	return context.WithValue(ctx, ctxKey, mask)
}

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

		return fn(
			MaskContext(ctx, mask),
			&pruningConn{
				StreamingHandlerConn: h,
				fm:                   mask,
			},
		)
	}

}

type pruningConn struct {
	connect.StreamingHandlerConn
	fm *FieldMask
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

		rsp, err := fn(MaskContext(ctx, mask), req)
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
