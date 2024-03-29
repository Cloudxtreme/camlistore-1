/*
Copyright 2011 Google Inc.

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

package index

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/images"
	"camlistore.org/pkg/jsonsign"
	"camlistore.org/pkg/magic"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/types"

	"camlistore.org/third_party/taglib"
)

var reindexMu sync.Mutex

func (ix *Index) reindex(br blob.Ref) {
	// TODO: cap how many of these can be going at once, probably more than 1,
	// and be more efficient than just blocking goroutines. For now, this:
	reindexMu.Lock()
	defer reindexMu.Unlock()

	bs := ix.BlobSource
	if bs == nil {
		log.Printf("index: can't re-index %v: no BlobSource", br)
		return
	}
	log.Printf("index: starting re-index of %v", br)
	rc, _, err := bs.FetchStreaming(br)
	if err != nil {
		log.Printf("index: failed to fetch %v for reindexing: %v", br, err)
		return
	}
	defer rc.Close()
	sb, err := blobserver.Receive(ix, br, rc)
	if err != nil {
		log.Printf("index: reindex of %v failed: %v", br, err)
		return
	}
	log.Printf("index: successfully reindexed %v", sb)
}

type mutationMap struct {
	kv map[string]string // the keys and values we populate
	// TODO(mpl): we only need to keep track of one claim so far,
	// but I chose a slice for when we need to do multi-claims?
	deletes []schema.Claim // we record if we get a delete claim, so we can update
	// the deletes cache right after committing the mutation.
}

func (mm *mutationMap) Set(k, v string) {
	if mm.kv == nil {
		mm.kv = make(map[string]string)
	}
	mm.kv[k] = v
}

func (mm *mutationMap) noteDelete(deleteClaim schema.Claim) {
	mm.deletes = append(mm.deletes, deleteClaim)
}

func (ix *Index) ReceiveBlob(blobRef blob.Ref, source io.Reader) (retsb blob.SizedRef, err error) {
	sniffer := NewBlobSniffer(blobRef)
	written, err := io.Copy(sniffer, source)
	if err != nil {
		return
	}

	sniffer.Parse()

	mm, err := ix.populateMutationMap(blobRef, sniffer)
	if err != nil {
		return
	}

	if err := ix.commit(mm); err != nil {
		return retsb, err
	}

	if c := ix.corpus; c != nil {
		if err = c.addBlob(blobRef, mm); err != nil {
			return
		}
	}

	// TODO(bradfitz): log levels? These are generally noisy
	// (especially in tests, like search/handler_test), but I
	// could see it being useful in production. For now, disabled:
	//
	// mimeType := sniffer.MIMEType()
	// log.Printf("indexer: received %s; type=%v; truncated=%v", blobRef, mimeType, sniffer.IsTruncated())

	return blob.SizedRef{blobRef, written}, nil
}

// commit writes the contents of the mutationMap on a batch
// mutation and commits that batch. It also updates the deletes
// cache.
func (ix *Index) commit(mm *mutationMap) error {
	// We want the update of the deletes cache to be atomic
	// with the transaction commit, so we lock here instead
	// of within updateDeletesCache.
	ix.deletes.Lock()
	defer ix.deletes.Unlock()
	bm := ix.s.BeginBatch()
	for k, v := range mm.kv {
		bm.Set(k, v)
	}
	err := ix.s.CommitBatch(bm)
	if err != nil {
		return err
	}
	for _, cl := range mm.deletes {
		if err := ix.updateDeletesCache(cl); err != nil {
			return fmt.Errorf("Could not update the deletes cache after deletion from %v: %v", cl, err)
		}
	}
	return nil
}

// populateMutationMap populates keys & values that will be committed
// into the returned map.
//
// the blobref can be trusted at this point (it's been fully consumed
// and verified to match), and the sniffer has been populated.
func (ix *Index) populateMutationMap(br blob.Ref, sniffer *BlobSniffer) (*mutationMap, error) {
	// TODO(mpl): shouldn't we remove these two from the map (so they don't get committed) when
	// e.g in populateClaim we detect a bogus claim (which does not yield an error)?
	mm := &mutationMap{
		kv: map[string]string{
			"have:" + br.String(): fmt.Sprintf("%d", sniffer.Size()),
			"meta:" + br.String(): fmt.Sprintf("%d|%s", sniffer.Size(), sniffer.MIMEType()),
		},
	}

	if blob, ok := sniffer.SchemaBlob(); ok {
		switch blob.Type() {
		case "claim":
			if err := ix.populateClaim(blob, mm); err != nil {
				return nil, err
			}
		case "file":
			if err := ix.populateFile(blob, mm); err != nil {
				return nil, err
			}
		case "directory":
			if err := ix.populateDir(blob, mm); err != nil {
				return nil, err
			}
		}
	}
	return mm, nil
}

// keepFirstN keeps the first N bytes written to it in Bytes.
type keepFirstN struct {
	N     int
	Bytes []byte
}

func (w *keepFirstN) Write(p []byte) (n int, err error) {
	if n := w.N - len(w.Bytes); n > 0 {
		if n > len(p) {
			n = len(p)
		}
		w.Bytes = append(w.Bytes, p[:n]...)
	}
	return len(p), nil
}

// b: the parsed file schema blob
// mm: keys to populate
func (ix *Index) populateFile(b *schema.Blob, mm *mutationMap) error {
	var times []time.Time // all creation or mod times seen; may be zero
	times = append(times, b.ModTime())

	blobRef := b.BlobRef()
	seekFetcher := blob.SeekerFromStreamingFetcher(ix.BlobSource)
	fr, err := b.NewFileReader(seekFetcher)
	if err != nil {
		// TODO(bradfitz): propagate up a transient failure
		// error type, so we can retry indexing files in the
		// future if blobs are only temporarily unavailable.
		// Basically the same as the TODO just below.
		log.Printf("index: error indexing file, creating NewFileReader %s: %v", blobRef, err)
		return nil
	}
	defer fr.Close()
	mime, reader := magic.MIMETypeFromReader(fr)

	sha1 := sha1.New()
	var copyDest io.Writer = sha1
	var imageBuf *keepFirstN // or nil
	if strings.HasPrefix(mime, "image/") {
		imageBuf = &keepFirstN{N: 256 << 10}
		copyDest = io.MultiWriter(copyDest, imageBuf)
	}
	size, err := io.Copy(copyDest, reader)
	if err != nil {
		// TODO: job scheduling system to retry this spaced
		// out max n times.  Right now our options are
		// ignoring this error (forever) or returning the
		// error and making the indexing try again (likely
		// forever failing).  Both options suck.  For now just
		// log and act like all's okay.
		log.Printf("index: error indexing file %s: %v", blobRef, err)
		return nil
	}

	if imageBuf != nil {
		if conf, err := images.DecodeConfig(bytes.NewReader(imageBuf.Bytes)); err == nil {
			mm.Set(keyImageSize.Key(blobRef), keyImageSize.Val(fmt.Sprint(conf.Width), fmt.Sprint(conf.Height)))
		}
		if ft, err := schema.FileTime(bytes.NewReader(imageBuf.Bytes)); err == nil {
			log.Printf("filename %q exif = %v, %v", b.FileName(), ft, err)
			times = append(times, ft)
		} else {
			log.Printf("filename %q exif = %v, %v", b.FileName(), ft, err)
		}
	}

	var sortTimes []time.Time
	for _, t := range times {
		if !t.IsZero() {
			sortTimes = append(sortTimes, t)
		}
	}
	sort.Sort(types.ByTime(sortTimes))
	var time3339s string
	switch {
	case len(sortTimes) == 1:
		time3339s = types.Time3339(sortTimes[0]).String()
	case len(sortTimes) >= 2:
		oldest, newest := sortTimes[0], sortTimes[len(sortTimes)-1]
		time3339s = types.Time3339(oldest).String() + "," + types.Time3339(newest).String()
	}

	wholeRef := blob.RefFromHash(sha1)
	mm.Set(keyWholeToFileRef.Key(wholeRef, blobRef), "1")
	mm.Set(keyFileInfo.Key(blobRef), keyFileInfo.Val(size, b.FileName(), mime))
	mm.Set(keyFileTimes.Key(blobRef), keyFileTimes.Val(time3339s))

	if strings.HasPrefix(mime, "audio/") {
		tag, err := taglib.Decode(fr, fr.Size())
		if err == nil {
			indexMusic(tag, wholeRef, mm)
		} else {
			log.Print("index: error parsing tag: ", err)
		}
	}

	return nil
}

// indexMusic adds mutations to index the wholeRef by most of the
// fields in gotaglib.GenericTag.
func indexMusic(tag taglib.GenericTag, wholeRef blob.Ref, mm *mutationMap) {
	const justYearLayout = "2006"

	var yearStr, trackStr string
	if !tag.Year().IsZero() {
		yearStr = tag.Year().Format(justYearLayout)
	}
	if tag.Track() != 0 {
		trackStr = fmt.Sprintf("%d", tag.Track())
	}

	tags := map[string]string{
		"title":  tag.Title(),
		"artist": tag.Artist(),
		"album":  tag.Album(),
		"genre":  tag.Genre(),
		"year":   yearStr,
		"track":  trackStr,
	}

	for tag, value := range tags {
		if value != "" {
			mm.Set(keyAudioTag.Key(tag, strings.ToLower(value), wholeRef), "1")
		}
	}
}

// b: the parsed file schema blob
// mm: keys to populate
func (ix *Index) populateDir(b *schema.Blob, mm *mutationMap) error {
	blobRef := b.BlobRef()
	// TODO(bradfitz): move the NewDirReader and FileName method off *schema.Blob and onto

	seekFetcher := blob.SeekerFromStreamingFetcher(ix.BlobSource)
	dr, err := b.NewDirReader(seekFetcher)
	if err != nil {
		// TODO(bradfitz): propagate up a transient failure
		// error type, so we can retry indexing files in the
		// future if blobs are only temporarily unavailable.
		log.Printf("index: error indexing directory, creating NewDirReader %s: %v", blobRef, err)
		return nil
	}
	sts, err := dr.StaticSet()
	if err != nil {
		log.Printf("index: error indexing directory: can't get StaticSet: %v\n", err)
		return nil
	}

	mm.Set(keyFileInfo.Key(blobRef), keyFileInfo.Val(len(sts), b.FileName(), ""))
	for _, br := range sts {
		mm.Set(keyStaticDirChild.Key(blobRef, br.String()), "1")
	}
	return nil
}

// populateDeleteClaim adds to mm the entries resulting from the delete claim cl.
// It is assumed cl is a valid claim, and vr has already been verified.
func (ix *Index) populateDeleteClaim(cl schema.Claim, vr *jsonsign.VerifyRequest, mm *mutationMap) {
	br := cl.Blob().BlobRef()
	target := cl.Target()
	if !target.Valid() {
		log.Print(fmt.Errorf("no valid target for delete claim %v", br))
		return
	}
	meta, err := ix.GetBlobMeta(target)
	if err != nil {
		if err == os.ErrNotExist {
			// TODO: return a dependency error type, to schedule re-indexing in the future
		}
		log.Print(fmt.Errorf("Could not get mime type of target blob %v: %v", target, err))
		return
	}
	// TODO(mpl): create consts somewhere for "claim" and "permanode" as camliTypes, and use them,
	// instead of hardcoding. Unless they already exist ? (didn't find them).
	if meta.CamliType != "permanode" && meta.CamliType != "claim" {
		log.Print(fmt.Errorf("delete claim target in %v is neither a permanode nor a claim: %v", br, meta.CamliType))
		return
	}
	mm.Set(keyDeleted.Key(target, cl.ClaimDateString(), br), "")
	if meta.CamliType == "claim" {
		return
	}
	recentKey := keyRecentPermanode.Key(vr.SignerKeyId, cl.ClaimDateString(), br)
	mm.Set(recentKey, target.String())
	attr, value := cl.Attribute(), cl.Value()
	claimKey := keyPermanodeClaim.Key(target, vr.SignerKeyId, cl.ClaimDateString(), br)
	mm.Set(claimKey, keyPermanodeClaim.Val(cl.ClaimType(), attr, value, vr.CamliSigner))
}

func (ix *Index) populateClaim(b *schema.Blob, mm *mutationMap) error {
	br := b.BlobRef()

	claim, ok := b.AsClaim()
	if !ok {
		// Skip bogus claim with malformed permanode.
		return nil
	}

	vr := jsonsign.NewVerificationRequest(b.JSON(), ix.KeyFetcher)
	if !vr.Verify() {
		// TODO(bradfitz): ask if the vr.Err.(jsonsign.Error).IsPermanent() and retry
		// later if it's not permanent? or maybe do this up a level?
		if vr.Err != nil {
			return vr.Err
		}
		return errors.New("index: populateClaim verification failure")
	}
	verifiedKeyId := vr.SignerKeyId
	mm.Set("signerkeyid:"+vr.CamliSigner.String(), verifiedKeyId)

	if claim.ClaimType() == string(schema.DeleteClaim) {
		ix.populateDeleteClaim(claim, vr, mm)
		mm.noteDelete(claim)
		return nil
	}

	pnbr := claim.ModifiedPermanode()
	if !pnbr.Valid() {
		// A different type of claim; not modifying a permanode.
		return nil
	}

	attr, value := claim.Attribute(), claim.Value()
	recentKey := keyRecentPermanode.Key(verifiedKeyId, claim.ClaimDateString(), br)
	mm.Set(recentKey, pnbr.String())
	claimKey := keyPermanodeClaim.Key(pnbr, verifiedKeyId, claim.ClaimDateString(), br)
	mm.Set(claimKey, keyPermanodeClaim.Val(claim.ClaimType(), attr, value, vr.CamliSigner))

	if strings.HasPrefix(attr, "camliPath:") {
		targetRef, ok := blob.Parse(value)
		if ok {
			// TODO: deal with set-attribute vs. del-attribute
			// properly? I think we get it for free when
			// del-attribute has no Value, but we need to deal
			// with the case where they explicitly delete the
			// current value.
			suffix := attr[len("camliPath:"):]
			active := "Y"
			if claim.ClaimType() == "del-attribute" {
				active = "N"
			}
			baseRef := pnbr
			claimRef := br

			key := keyPathBackward.Key(verifiedKeyId, targetRef, claimRef)
			val := keyPathBackward.Val(claim.ClaimDateString(), baseRef, active, suffix)
			mm.Set(key, val)

			key = keyPathForward.Key(verifiedKeyId, baseRef, suffix, claim.ClaimDateString(), claimRef)
			val = keyPathForward.Val(active, targetRef)
			mm.Set(key, val)
		}
	}

	if claim.ClaimType() != string(schema.DelAttributeClaim) && IsIndexedAttribute(attr) {
		key := keySignerAttrValue.Key(verifiedKeyId, attr, value, claim.ClaimDateString(), br)
		mm.Set(key, keySignerAttrValue.Val(pnbr))
	}

	if IsBlobReferenceAttribute(attr) {
		targetRef, ok := blob.Parse(value)
		if ok {
			key := keyEdgeBackward.Key(targetRef, pnbr, br)
			mm.Set(key, keyEdgeBackward.Val("permanode", ""))
		}
	}

	return nil
}

// updateDeletesCache updates the index deletes cache with the cl delete claim.
// deleteClaim is trusted to be a valid delete Claim.
func (x *Index) updateDeletesCache(deleteClaim schema.Claim) error {
	target := deleteClaim.Target()
	deleter := deleteClaim.Blob()
	when, err := deleter.ClaimDate()
	if err != nil {
		return fmt.Errorf("Could not get date of delete claim %v: %v", deleteClaim, err)
	}
	targetDeletions := append(x.deletes.m[target],
		deletion{
			deleter: deleter.BlobRef(),
			when:    when,
		})
	sort.Sort(sort.Reverse(byDeletionDate(targetDeletions)))
	x.deletes.m[target] = targetDeletions
	return nil
}
