package masks_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/hxtk/aip/internal/testpb"
	"github.com/hxtk/aip/internal/testpb/testpbconnect"
	"github.com/hxtk/aip/masks"
)

// fake service with both unary and streaming methods
type fakeBookService struct{}

func (s *fakeBookService) GetBook(
	ctx context.Context,
	req *connect.Request[testpb.GetBookRequest],
) (*connect.Response[testpb.Book], error) {
	// always return a Book with too many fields set
	return connect.NewResponse(&testpb.Book{
		Title:  "keep",
		Name:   "drop",
		Author: &testpb.Author{GivenName: "keep", FamilyName: "drop"},
	}), nil
}

func (s *fakeBookService) ListBooks(
	ctx context.Context,
	req *connect.Request[testpb.ListBooksRequest],
	stream *connect.ServerStream[testpb.Book],
) error {
	// send one Book message
	return stream.Send(&testpb.Book{
		Title:  "keep",
		Name:   "drop",
		Author: &testpb.Author{GivenName: "keep", FamilyName: "drop"},
	})
}

func newTestServer() *httptest.Server {
	svc := &fakeBookService{}
	mux := http.NewServeMux()
	mux.Handle(testpbconnect.NewBookServiceHandler(
		svc,
		connect.WithInterceptors(masks.WithReadMaskInterceptor("x-goog-fieldmask")),
	))
	return httptest.NewServer(mux)
}

func TestUnaryInterceptorE2E(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := testpbconnect.NewBookServiceClient(
		http.DefaultClient,
		srv.URL,
	)

	req := connect.NewRequest(&testpb.GetBookRequest{Name: "book1"})
	req.Header().Set("x-goog-fieldmask", "title,author.given_name")

	res, err := client.GetBook(context.Background(), req)
	if err != nil {
		t.Fatalf("GetBook failed: %v", err)
	}

	book := res.Msg
	if book.Title == "" {
		t.Errorf("expected Title kept")
	}
	if book.Name != "" {
		t.Errorf("expected Name cleared, got %q", book.Name)
	}
	if book.Author.FamilyName != "" {
		t.Errorf("expected Author.FamilyName cleared, got %q", book.Author.FamilyName)
	}
}

func TestServerStreamInterceptorE2E(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	client := testpbconnect.NewBookServiceClient(
		http.DefaultClient,
		srv.URL,
	)

	req := connect.NewRequest(&testpb.ListBooksRequest{PageSize: 1})
	req.Header().Set("x-goog-fieldmask", "title,author.given_name")

	stream, err := client.ListBooks(context.Background(), req)
	if err != nil {
		t.Fatalf("ListBooks failed: %v", err)
	}

	if !stream.Receive() {
		if stream.Err() != nil {
			t.Fatalf("stream error: %v", stream.Err())
		}
		t.Fatal("expected one message from stream, got none")
	}

	book := stream.Msg()
	if book.Title == "" {
		t.Errorf("expected Title kept")
	}
	if book.Name != "" {
		t.Errorf("expected Name cleared, got %q", book.Name)
	}
	if book.Author.FamilyName != "" {
		t.Errorf("expected Author.FamilyName cleared, got %q", book.Author.FamilyName)
	}
}
