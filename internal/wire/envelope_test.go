package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// The golden values below were produced by the TypeScript reference
// implementation (protobus 2.4.0, MessageFactory) — not derived by hand. They
// pin the Go encoder to the exact bytes a TS peer emits, so a mixed deployment
// cannot drift silently. A Go peer that only had to *decode* TS bytes could get
// away with less, but byte parity also keeps captured traffic, DLQ contents
// and golden tests comparable across all three ports.

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func TestAppendRequestMatchesTypeScript(t *testing.T) {
	tests := []struct {
		name   string
		req    Request
		golden string
	}{
		{
			name:   "no actor",
			req:    Request{Method: "T.Svc.add", Data: []byte{0x08, 0x01}},
			golden: "0a09542e5376632e6164641a020801",
		},
		{
			// proto3 would drop an all-default payload; TS always writes the
			// data field, even empty. Matching that keeps the bytes identical.
			name:   "empty payload is still emitted",
			req:    Request{Method: "T.Svc.add", Actor: "x", Data: nil},
			golden: "0a09542e5376632e6164641201781a00",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AppendRequest(nil, tc.req)
			if want := mustHex(t, tc.golden); !bytes.Equal(got, want) {
				t.Fatalf("encoding mismatch\n got %x\nwant %x", got, want)
			}
		})
	}
}

func TestAppendResponseMatchesTypeScript(t *testing.T) {
	tests := []struct {
		name   string
		resp   Response
		golden string
	}{
		{
			name:   "result",
			resp:   Response{Result: &Result{Method: "T.Svc.add", Data: []byte{0x08, 0x02}}},
			golden: "0a0f0a09542e5376632e61646412020802",
		},
		{
			name:   "result with an all-default message",
			resp:   Response{Result: &Result{Method: "T.Svc.add"}},
			golden: "0a0d0a09542e5376632e6164641200",
		},
		{
			// An empty code is written explicitly, as TS does.
			name:   "error without code",
			resp:   Response{Error: &Error{Method: "T.Svc.add", Message: "boom"}},
			golden: "12130a09542e5376632e6164641204626f6f6d1a00",
		},
		{
			name:   "error with code",
			resp:   Response{Error: &Error{Method: "T.Svc.add", Message: "m", Code: "C"}},
			golden: "12110a09542e5376632e61646412016d1a0143",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AppendResponse(nil, tc.resp)
			if err != nil {
				t.Fatal(err)
			}
			if want := mustHex(t, tc.golden); !bytes.Equal(got, want) {
				t.Fatalf("encoding mismatch\n got %x\nwant %x", got, want)
			}
		})
	}
}

func TestAppendResponseRejectsAmbiguousContainer(t *testing.T) {
	if _, err := AppendResponse(nil, Response{}); err == nil {
		t.Fatal("a response with neither result nor error must not encode")
	}
	both := Response{Result: &Result{Method: "a"}, Error: &Error{Method: "a"}}
	if _, err := AppendResponse(nil, both); err == nil {
		t.Fatal("a response with both result and error must not encode")
	}
}

func TestAppendEventMatchesTypeScript(t *testing.T) {
	got := AppendEvent(nil, Event{Type: "T.Ev", Topic: "EVENT.T.Ev", Data: []byte{0x0a, 0x01, 0x79}})
	want := mustHex(t, "0a04542e4576120a4556454e542e542e45761a030a0179")
	if !bytes.Equal(got, want) {
		t.Fatalf("encoding mismatch\n got %x\nwant %x", got, want)
	}
}

func TestDecodeRequestFromTypeScript(t *testing.T) {
	// actor written as an explicit empty string by TS
	req, err := DecodeRequest(mustHex(t, "0a09542e5376632e61646412001a020801"))
	if err != nil {
		t.Fatal(err)
	}
	if req.Method != "T.Svc.add" || req.Actor != "" || !bytes.Equal(req.Data, []byte{0x08, 0x01}) {
		t.Fatalf("unexpected request %+v", req)
	}
}

func TestDecodeResponseFromTypeScript(t *testing.T) {
	resp, err := DecodeResponse(mustHex(t, "12110a09542e5376632e61646412016d1a0143"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Result != nil {
		t.Fatalf("want an error response, got %+v", resp)
	}
	if *resp.Error != (Error{Method: "T.Svc.add", Message: "m", Code: "C"}) {
		t.Fatalf("unexpected error %+v", *resp.Error)
	}

	resp, err = DecodeResponse(mustHex(t, "0a0d0a09542e5376632e6164641200"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result == nil || resp.Result.Method != "T.Svc.add" || len(resp.Result.Data) != 0 {
		t.Fatalf("unexpected result %+v", resp)
	}
}

func TestDecodeResponseErrorWinsOverResult(t *testing.T) {
	// TS checks `error` first; a container carrying both must read as an error
	// on every port, or the same bytes mean success in one language and
	// failure in another.
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, AppendResult(nil, Result{Method: "a.B.c"}))
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, appendError(nil, Error{Method: "a.B.c", Message: "no"}))
	resp, err := DecodeResponse(b)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Result != nil {
		t.Fatalf("error must win, got %+v", resp)
	}
}

func TestDecodeResponseEmptyContainer(t *testing.T) {
	if _, err := DecodeResponse(nil); err == nil {
		t.Fatal("an empty container carries neither result nor error and must be refused")
	}
}

func TestDecodeEventFromTypeScript(t *testing.T) {
	ev, err := DecodeEvent(mustHex(t, "0a04542e4576120a4556454e542e542e45761a030a0179"))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type != "T.Ev" || ev.Topic != "EVENT.T.Ev" || !bytes.Equal(ev.Data, []byte{0x0a, 0x01, 0x79}) {
		t.Fatalf("unexpected event %+v", ev)
	}
}

func TestDecodeSkipsUnknownFields(t *testing.T) {
	// A future peer adding a field must not break an older reader.
	b := AppendRequest(nil, Request{Method: "a.B.c", Data: []byte{1}})
	b = protowire.AppendTag(b, 99, protowire.VarintType)
	b = protowire.AppendVarint(b, 7)
	req, err := DecodeRequest(b)
	if err != nil {
		t.Fatal(err)
	}
	if req.Method != "a.B.c" {
		t.Fatalf("unexpected request %+v", req)
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	cases := map[string][]byte{
		"truncated length":     {0x0a, 0x09, 'T'},
		"bad tag":              {0x00},
		"wrong wire type":      {0x08, 0x01}, // field 1 as varint, not bytes
		"truncated varint":     {0x0a, 0xff},
		"invalid utf8 in name": {0x0a, 0x01, 0xff},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRequest(b); err == nil {
				t.Fatalf("expected an error for %x", b)
			}
		})
	}
}

func TestDecodedDataDoesNotAliasMutableCallerMemoryOnEncode(t *testing.T) {
	data := []byte{1, 2, 3}
	b := AppendRequest(nil, Request{Method: "a.B.c", Data: data})
	data[0] = 9
	req, err := DecodeRequest(b)
	if err != nil {
		t.Fatal(err)
	}
	if req.Data[0] != 1 {
		t.Fatal("encoding must copy the payload into the output buffer")
	}
}

func TestRoundTrip(t *testing.T) {
	in := Request{Method: "pkg.sub.Service.method", Actor: "user:42", Data: bytes.Repeat([]byte{0xab}, 300)}
	out, err := DecodeRequest(AppendRequest(nil, in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Method != in.Method || out.Actor != in.Actor || !bytes.Equal(out.Data, in.Data) {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestAppendReusesCapacity(t *testing.T) {
	buf := make([]byte, 0, 128)
	got := AppendRequest(buf, Request{Method: "a.B.c", Data: []byte{1}})
	if &got[0] != &buf[:1][0] {
		t.Fatal("Append must write into the caller's spare capacity")
	}
}

func FuzzDecodeRequest(f *testing.F) {
	f.Add(AppendRequest(nil, Request{Method: "a.B.c", Actor: "x", Data: []byte{1, 2}}))
	f.Add([]byte{0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, b []byte) {
		req, err := DecodeRequest(b)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode and decode to the same value.
		again, err := DecodeRequest(AppendRequest(nil, req))
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if again.Method != req.Method || again.Actor != req.Actor || !bytes.Equal(again.Data, req.Data) {
			t.Fatalf("unstable round trip: %+v vs %+v", req, again)
		}
	})
}

func FuzzDecodeResponse(f *testing.F) {
	ok, _ := AppendResponse(nil, Response{Result: &Result{Method: "a.B.c", Data: []byte{1}}})
	f.Add(ok)
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = DecodeResponse(b) })
}

func FuzzDecodeEvent(f *testing.F) {
	f.Add(AppendEvent(nil, Event{Type: "a.B", Topic: "EVENT.a.B", Data: []byte{1}}))
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = DecodeEvent(b) })
}
