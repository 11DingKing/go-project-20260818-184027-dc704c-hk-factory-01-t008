package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// handleSubscribe streams events to a department subscriber using Server-Sent
// Events. The client specifies a subscriber_id and optionally an offset to
// resume from. On disconnect, the committed offset allows reconnect to
// continue from the last seen position.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	topic := "topic." + code

	subscriberID := r.URL.Query().Get("subscriber_id")
	if subscriberID == "" {
		subscriberID = "sub-" + code
	}

	startOffset := int64(0)
	if off := r.URL.Query().Get("offset"); off != "" {
		if n, err := strconv.ParseInt(off, 10, 64); err == nil {
			startOffset = n
		}
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		committed, err := s.orch.GetSubscriberOffset(ctx, subscriberID, topic)
		if err == nil {
			startOffset = committed
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal", "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	currentOffset := startOffset
	emptyTicks := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entries, err := s.orch.ReadEventLog(ctx, topic, currentOffset, 50)
		if err != nil {
			fmt.Fprintf(w, "event: error\ndata: %s\n\n", jsonString(map[string]string{"error": err.Error()}))
			flusher.Flush()
			return
		}

		if len(entries) == 0 {
			emptyTicks++
			if emptyTicks > 120 {
				fmt.Fprintf(w, ": keepalive\n\n")
				flusher.Flush()
				emptyTicks = 0
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		emptyTicks = 0

		for _, entry := range entries {
			data, _ := json.Marshal(entry)
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", entry.Offset, entry.EventType, data)
			flusher.Flush()
			currentOffset = entry.Offset

			subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = s.orch.CommitSubscriberOffset(subCtx, subscriberID, topic, entry.Offset)
			cancel()
		}
	}
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
