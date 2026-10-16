package server

import (
	"context"
	"errors"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/grpc/chunk"
	webserverv1 "github.com/sourcegraph/zoekt/grpc/protos/zoekt/webserver/v1"
	"github.com/sourcegraph/zoekt/query"
)

func NewServer(s zoekt.Streamer) *Server {
	return &Server{streamer: s}
}

type Server struct {
	webserverv1.UnimplementedWebserverServiceServer
	streamer zoekt.Streamer
}

func (s *Server) Search(ctx context.Context, req *webserverv1.SearchRequest) (*webserverv1.SearchResponse, error) {
	q, err := query.QFromProto(req.GetQuery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	res, err := s.streamer.Search(ctx, q, zoekt.SearchOptionsFromProto(req.GetOpts()))
	if err != nil {
		return nil, err
	}
	return res.ToProto(), nil
}

func (s *Server) List(ctx context.Context, req *webserverv1.ListRequest) (*webserverv1.ListResponse, error) {
	q, err := query.QFromProto(req.GetQuery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	repoList, err := s.streamer.List(ctx, q, zoekt.ListOptionsFromProto(req.GetOpts()))
	if err != nil {
		return nil, err
	}

	return repoList.ToProto(), nil
}

func (s *Server) StreamSearch(req *webserverv1.StreamSearchRequest, ss webserverv1.WebserverService_StreamSearchServer) error {
	var (
		q       query.Q
		opts    *zoekt.SearchOptions
		haveReq = req.GetRequest() != nil
	)
	if haveReq {
		var err error
		q, err = query.QFromProto(req.GetRequest().GetQuery())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		opts = zoekt.SearchOptionsFromProto(req.GetRequest().GetOpts())
	}

	token := req.GetResumeToken()
	isResumeOrAbandon := len(token) > 0 || req.GetAbandonSnapshot()

	if isResumeOrAbandon {
		return s.resumeOrAbandon(ss, q, opts, haveReq, req)
	}

	if rs, ok := s.streamer.(zoekt.ResumableStreamer); ok {
		// Snapshot-backed streaming: execution is identical, but every safe
		// batch additionally carries an opaque resume credential.
		sender := newSamplingSender(gRPCChunkSender(ss))
		_, err := rs.StreamSearchResume(ss.Context(), q, opts,
			&zoekt.StreamResumeRequest{}, zoekt.SenderFunc(sender.Send))
		if err == nil {
			sender.Flush()
		}
		return err
	}

	// Backends that do not implement resumable streaming keep the legacy path.
	sender := newSamplingSender(gRPCChunkSender(ss))
	err := s.streamer.StreamSearch(ss.Context(), q, opts, zoekt.SenderFunc(sender.Send))
	if err == nil {
		sender.Flush()
	}
	return err
}

// resumeOrAbandon handles a request that carries a resume token and/or asks to
// abandon the snapshot.
func (s *Server) resumeOrAbandon(
	ss webserverv1.WebserverService_StreamSearchServer,
	q query.Q,
	opts *zoekt.SearchOptions,
	haveReq bool,
	req *webserverv1.StreamSearchRequest,
) error {
	rs, ok := s.streamer.(zoekt.ResumableStreamer)
	if !ok {
		return status.Error(codes.FailedPrecondition, "backend does not support resumable streaming")
	}
	if len(req.GetResumeToken()) == 0 {
		return status.Error(codes.InvalidArgument, "abandon_snapshot requires resume_token")
	}
	if !haveReq {
		return status.Error(codes.InvalidArgument, "resume request must echo the original query and options")
	}

	sender := newSamplingSender(gRPCChunkSender(ss))
	abandoned, err := rs.StreamSearchResume(ss.Context(), q, opts,
		&zoekt.StreamResumeRequest{Token: req.GetResumeToken(), Abandon: req.GetAbandonSnapshot()},
		zoekt.SenderFunc(sender.Send))
	if err != nil {
		var resumeErr *zoekt.StreamResumeError
		if errors.As(err, &resumeErr) {
			// Terminal, distinguishable non-resumable outcome: no search was
			// executed and the client must resubmit the query itself.
			return ss.Send(&webserverv1.StreamSearchResponse{
				ResumeError: mapResumeError(resumeErr.Code),
			})
		}
		return err
	}
	sender.Flush()
	if abandoned {
		return ss.Send(&webserverv1.StreamSearchResponse{SnapshotAbandoned: true})
	}
	return nil
}

// mapResumeError maps a zoekt resume error code to the wire enum.
func mapResumeError(code zoekt.StreamResumeErrorCode) webserverv1.StreamResumeError {
	switch code {
	case zoekt.StreamResumeInvalidToken:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_INVALID_TOKEN
	case zoekt.StreamResumeMismatch:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_MISMATCH
	case zoekt.StreamResumeExpired:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_EXPIRED
	case zoekt.StreamResumeRestarted:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_RESTARTED
	case zoekt.StreamResumeCompleted:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_COMPLETED
	case zoekt.StreamResumeAbandoned:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_ABANDONED
	default:
		return webserverv1.StreamResumeError_STREAM_RESUME_ERROR_UNSPECIFIED
	}
}

// gRPCChunkSender is a zoekt.Sender that sends the file matches to the provided
// gRPC server stream. A resume credential attached to the SearchResult rides
// only the final chunk message of the batch, so receiving it confirms every
// file of the batch (and every earlier batch).
func gRPCChunkSender(ss webserverv1.WebserverService_StreamSearchServer) zoekt.Sender {
	f := func(r *zoekt.SearchResult) {
		result := r.ToStreamProto().GetResponseChunk()

		if len(result.GetFiles()) == 0 { // stats-only result, send it immediately
			_ = ss.Send(&webserverv1.StreamSearchResponse{
				ResponseChunk: result,
				ResumeToken:   r.ResumeToken,
			})
			return
		}

		statsSent := false
		numFilesSent := 0

		sendFunc := func(filesChunk []*webserverv1.FileMatch) error {
			var stats *webserverv1.Stats
			if !statsSent { // Stats are sent on the first chunk only
				stats = result.GetStats()
				statsSent = true
			}

			progress := result.GetProgress()

			// We want the client to consume the entire set of chunks - so we
			// manually patch MaxPendingPriority >= overall priority, ensuring
			// the client doesn't stop consuming chunks early.
			if numFilesSent+len(filesChunk) < len(result.GetFiles()) {
				progress = proto.Clone(progress).(*webserverv1.Progress)
				progress.MaxPendingPriority = math.Max(
					result.GetProgress().GetPriority(),
					result.GetProgress().GetMaxPendingPriority(),
				)
			}

			out := &webserverv1.StreamSearchResponse{
				ResponseChunk: &webserverv1.SearchResponse{
					Stats:    stats,
					Progress: progress,

					Files: filesChunk,
				},
			}
			numFilesSent += len(filesChunk)
			if numFilesSent == len(result.GetFiles()) {
				out.ResumeToken = r.ResumeToken
			}
			return ss.Send(out)
		}

		_ = chunk.SendAll(sendFunc, result.GetFiles()...)
	}
	return zoekt.SenderFunc(f)
}
