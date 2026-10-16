package server

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	webserverv1 "github.com/sourcegraph/zoekt/grpc/protos/zoekt/webserver/v1"
)

func TestResumeProtoDescriptor(t *testing.T) {
	msg := (&webserverv1.StreamSearchRequest{}).ProtoReflect().Descriptor()

	findField := func(n protoreflect.Name) protoreflect.FieldDescriptor {
		fields := msg.Fields()
		for i := 0; i < fields.Len(); i++ {
			if f := fields.Get(i); f.Name() == n {
				return f
			}
		}
		return nil
	}

	tok := findField("resume_token")
	if tok == nil || tok.Number() != 4 || tok.Kind() != protoreflect.BytesKind {
		t.Fatalf("resume_token field missing/wrong: %v", tok)
	}
	abandon := findField("abandon_snapshot")
	if abandon == nil || abandon.Number() != 5 || abandon.Kind() != protoreflect.BoolKind {
		t.Fatalf("abandon_snapshot field missing/wrong: %v", abandon)
	}

	resp := (&webserverv1.StreamSearchResponse{}).ProtoReflect().Descriptor()
	rf := resp.Fields().ByName("resume_token")
	if rf == nil || rf.Number() != 7 {
		t.Fatalf("response resume_token missing: %v", rf)
	}
	re := resp.Fields().ByName("resume_error")
	if re == nil || re.Number() != 8 || re.Kind() != protoreflect.EnumKind {
		t.Fatalf("response resume_error missing: %v", re)
	}
	ra := resp.Fields().ByName("snapshot_abandoned")
	if ra == nil || ra.Number() != 9 || ra.Kind() != protoreflect.BoolKind {
		t.Fatalf("response snapshot_abandoned missing: %v", ra)
	}

	// Every documented error value is represented in the enum.
	values := webserverv1.StreamResumeError_STREAM_RESUME_ERROR_UNSPECIFIED.Descriptor().Values()
	want := map[webserverv1.StreamResumeError]bool{
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_UNSPECIFIED: true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_INVALID_TOKEN: true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_MISMATCH:     true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_EXPIRED:      true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_RESTARTED:    true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_COMPLETED:    true,
		webserverv1.StreamResumeError_STREAM_RESUME_ERROR_ABANDONED:    true,
	}
	for i := 0; i < values.Len(); i++ {
		v := values.Get(i)
		code := webserverv1.StreamResumeError(v.Number())
		if !want[code] {
			t.Fatalf("unexpected enum value: %v", v.Name())
		}
		delete(want, code)
	}
	if len(want) != 0 {
		t.Fatalf("missing enum values: %v", want)
	}
}
