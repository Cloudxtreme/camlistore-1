/*
Copyright 2014 The Camlistore AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blobpacked

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/types"
	"camlistore.org/third_party/go/pkg/archive/zip"
)

// StreamBlobs impl.

// Continuation token is:
// "s*" if we're in the small blobs, (or "" to start):
//   "s:pt:<underlying token>" (pass through)
//   "s:after:<last-blobref-set>" (blob ref of already-sent item)
// "l*" if we're in the large blobs:
//   "l:<big-blobref,lexically>:<offset>" (of blob data from beginning of zip)
//   TODO: also care about whether large supports blob streamer?
// First it streams from small (if available, else enumerates)
// Then it streams from large (if available, else enumerates),
// and for each large, streams the contents of the zips.
func (s *storage) StreamBlobs(ctx *context.Context, dest chan<- *blob.Blob, contToken string, limitBytes int64) (nextContinueToken string, err error) {
	switch {
	case contToken == "" || strings.HasPrefix(contToken, "s:"):
		return s.streamSmallBlobs(ctx, dest, strings.TrimPrefix(contToken, "s:"), limitBytes)
	case strings.HasPrefix(contToken, "l:"):
		return s.streamLargeBlobs(ctx, dest, strings.TrimPrefix(contToken, "l:"), limitBytes)
	default:
		close(dest)
		return "", fmt.Errorf("invalid continue token %q", contToken)
	}
}

func (s *storage) streamSmallBlobs(ctx *context.Context, dest chan<- *blob.Blob, contToken string, limitBytes int64) (nextContinueToken string, err error) {
	stream := makeStreamer(s.small, "pt:", "after:")
	next, err := stream.StreamBlobs(ctx, dest, contToken, limitBytes)
	if next != "" {
		return "s:" + next, nil
	}
	return "l:", err
}

// This will violate the contract a little bit: will split on zip boundaries, so limitBytes
// may be overrun by the unzipped size of the max blob (16Mb).
func (s *storage) streamLargeBlobs(ctx *context.Context, dest chan<- *blob.Blob, contToken string, limitBytes int64) (nextContinueToken string, err error) {
	defer close(dest)
	stream := makeStreamer(s.large, "pt:", "after:")
	zips := make(chan *blob.Blob)
	go func() {
		nextContinueToken, err = stream.StreamBlobs(ctx, zips, contToken, limitBytes)
		if nextContinueToken != "" {
			nextContinueToken = "l:" + nextContinueToken
		}
	}()

	var slurp bytes.Buffer
	doneCh := ctx.Done()
	for zb := range zips {
		rsc := zb.Open()
		slurp.Reset()
		_, err := io.CopyN(&slurp, rsc, int64(zb.Size()))
		_ = rsc.Close()
		if err != nil {
			return nextContinueToken, err
		}
		zr, err := zip.NewReader(bytes.NewReader(slurp.Bytes()), int64(zb.Size()))
		if err != nil {
			return nextContinueToken, err
		}
		err = s.foreachZipBlobReader(zr, func(bp BlobAndPos, f *zip.File) error {
			select {
			case <-doneCh:
				return context.ErrCanceled
			default:
			}
			fr, err := f.Open()
			if err != nil {
				return err
			}
			defer fr.Close()
			b, err := ioutil.ReadAll(fr)
			if err != nil {
				return err
			}
			dest <- blob.NewBlob(bp.SizedRef.Ref, bp.SizedRef.Size,
				func() types.ReadSeekCloser {
					return struct {
						io.ReadSeeker
						io.Closer
					}{bytes.NewReader(b), ioutil.NopCloser(nil)}
				})
			return nil
		})
		rsc.Close()
		if err != nil {
			return nextContinueToken, err
		}
	}
	return "", err
}

type fetchEnumerator interface {
	blob.Fetcher
	blobserver.BlobEnumerator
}

type enumStream struct {
	enum                  fetchEnumerator
	ptPrefix, afterPrefix string
}

func makeStreamer(s fetchEnumerator, ptPrefix, afterPrefix string) blobserver.BlobStreamer {
	return &enumStream{enum: s, ptPrefix: ptPrefix, afterPrefix: afterPrefix}
}

func (s *enumStream) StreamBlobs(ctx *context.Context, dest chan<- *blob.Blob, contToken string, limitBytes int64) (nextContinueToken string, err error) {
	if stream, ok := s.enum.(blobserver.BlobStreamer); ok {
		if s.ptPrefix != "" && contToken != "" || !strings.HasPrefix(contToken, s.ptPrefix) {
			close(dest)
			return "", errors.New("invalid pass-through stream token")
		}
		return stream.StreamBlobs(ctx, dest, strings.TrimPrefix(contToken, s.ptPrefix), limitBytes)
	}
	defer close(dest)
	if s.afterPrefix != "" && contToken != "" && !strings.HasPrefix(contToken, s.afterPrefix) {
		return "", fmt.Errorf("invalid continue token %q", contToken)
	}
	enumCtx := ctx.New()
	enumDone := enumCtx.Done()
	defer enumCtx.Cancel()
	sbc := make(chan blob.SizedRef) // unbuffered
	enumErrc := make(chan error, 1)
	go func() {
		defer close(sbc)
		enumErrc <- blobserver.EnumerateAllFrom(enumCtx, s.enum, strings.TrimPrefix(contToken, s.afterPrefix), func(sb blob.SizedRef) error {
			select {
			case sbc <- sb:
				return nil
			case <-enumDone:
				return context.ErrCanceled
			}
		})
	}()
	var sent int64
	var lastRef blob.Ref
	for sent < limitBytes {
		sb, ok := <-sbc
		if !ok {
			break
		}
		opener := func() types.ReadSeekCloser {
			return blob.NewLazyReadSeekCloser(s.enum, sb.Ref)
		}
		select {
		case dest <- blob.NewBlob(sb.Ref, sb.Size, opener):
			lastRef = sb.Ref
			sent += int64(sb.Size)
		case <-ctx.Done():
			return "", context.ErrCanceled
		}
	}

	enumCtx.Cancel() // redundant if sbc was already closed, but harmless.
	enumErr := <-enumErrc
	if enumErr != nil || sent >= limitBytes {
		return s.afterPrefix + lastRef.String(), enumErr
	}
	return "", enumErr
}
