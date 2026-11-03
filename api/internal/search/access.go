package search

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type accessKey struct{}
type excludedKey struct{}

func WithExcludedStreams(ctx context.Context, names []string) context.Context {
	names = append([]string{}, names...)
	sort.Strings(names)
	return context.WithValue(ctx, excludedKey{}, names)
}

type searchAccess struct {
	Owner   string
	Streams []string
}

// WithAccess scopes both the query and its cursors. Nil streams means every
// stream; a non-nil empty list means none. The owner must be a stable user ID.
func WithAccess(ctx context.Context, owner string, streams []string) context.Context {
	if streams != nil {
		streams = append([]string{}, streams...)
		sort.Strings(streams)
	}
	return context.WithValue(ctx, accessKey{}, searchAccess{Owner: owner, Streams: streams})
}
func accessFingerprint(ctx context.Context) string {
	value, ok := ctx.Value(accessKey{}).(searchAccess)
	excluded, _ := ctx.Value(excludedKey{}).([]string)
	if !ok && len(excluded) == 0 {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Access   searchAccess
		Excluded []string
	}{value, excluded})
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}
