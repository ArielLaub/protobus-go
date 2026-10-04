package protobus

import (
	"bytes"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/ArielLaub/protobus-go/v2/internal/fakebroker"
	"github.com/ArielLaub/protobus-go/v2/internal/testpb"
	"github.com/ArielLaub/protobus-go/v2/pbtypes"
)

func overlong() *pbtypes.Bigint { return &pbtypes.Bigint{Value: bytes.Repeat([]byte{1}, 33)} }

func TestCheckCustomTypesFindsOverlongBigints(t *testing.T) {
	cases := map[string]proto.Message{
		"top level": &testpb.Order{Amount: overlong()},
		"repeated":  &testpb.Order{Parts: []*pbtypes.Bigint{pbtypes.BigintFromUint64(1), overlong()}},
		"map value": &testpb.Order{Balances: map[string]*pbtypes.Bigint{"k": overlong()}},
	}
	for name, m := range cases {
		if err := checkCustomTypes(m); !errors.Is(err, pbtypes.ErrBigintRange) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	if err := checkCustomTypes(&testpb.Order{Amount: pbtypes.BigintFromUint64(5)}); err != nil {
		t.Fatalf("a valid bigint: %v", err)
	}
	if err := checkCustomTypes(&testpb.AddRequest{A: 1}); err != nil {
		t.Fatalf("a message without custom types: %v", err)
	}
}

func TestOverlongBigintIsAProtocolErrorNotARetry(t *testing.T) {
	// The wire format is at most 32 bytes. Anything wider is malformed and is
	// answered like any undecodable payload: at once, never retried.
	b := fakebroker.New()
	bus := dialTest(t, b, fastConfig())
	startCalc(t, bus, newCalcImpl(), fastRetry(3))
	peer := newRawPeer(t, b)
	cid := peer.send("REQUEST.Test.Calc.echo", amqp.Publishing{Body: peer.request("Test.Calc.echo", &testpb.Order{Amount: overlong()})})
	er := decodeErrorReply(t, peer.await(cid, 1)[0].Body)
	if er.Code != CodeProtocol {
		t.Fatalf("got %+v", er)
	}
	for _, op := range b.OpsOf("publish") {
		if op.Exchange == "Test.Calc.Retry.Exchange" {
			t.Fatal("a malformed bigint must not be retried")
		}
	}
}
