// Command tokenstream demonstrates server-streaming with cancellation, in the
// shape a chat UI needs: a service streams tokens like a language model, and
// the caller stops it three ways.
//
//	docker compose up -d --wait
//	AMQP_URL=amqp://guest:guest@127.0.0.1:25672/ go run ./examples/tokenstream
//
// After each run the demo asks the SERVER how much it generated. Cancellation
// that only stopped the reader would show the full count; here the producer
// stops, because the caller's cancel reaches it as its context ending.
package main

//go:generate go run ../../cmd/protobus generate -proto ./proto -out ./gen

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	protobus "github.com/ArielLaub/protobus-go/v2"
	"github.com/ArielLaub/protobus-go/v2/examples/tokenstream/gen/chat"
)

// assistant streams a long canned completion, one word at a time.
type assistant struct {
	chat.UnimplementedAssistantServer
	mu           sync.Mutex
	generated    int32
	stoppedEarly bool
}

func (a *assistant) Generate(ctx context.Context, in *chat.GenerateRequest, stream protobus.ServerStream[*chat.Token]) error {
	words := completion(in.Prompt)
	delay := time.Duration(in.TokenDelayMs) * time.Millisecond
	if delay == 0 {
		delay = 60 * time.Millisecond
	}
	a.mu.Lock()
	a.generated, a.stoppedEarly = 0, false
	a.mu.Unlock()

	for i, w := range words {
		// ctx ends when the caller cancels: it broke out of its loop, its own
		// context ended, or it went idle. This is where a real service would
		// abort its upstream model call, and the point of the demo: stopping
		// saves the work, not just the reading.
		select {
		case <-ctx.Done():
			a.mu.Lock()
			a.stoppedEarly = true
			a.mu.Unlock()
			return nil
		case <-time.After(delay):
		}
		if err := stream.Send(&chat.Token{Index: int32(i), Text: w}); err != nil {
			return err
		}
		a.mu.Lock()
		a.generated = int32(i + 1)
		a.mu.Unlock()
	}
	return nil
}

func (a *assistant) Stats(context.Context, *chat.StatsRequest) (*chat.StatsResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return &chat.StatsResponse{TokensGenerated: a.generated, StoppedEarly: a.stoppedEarly}, nil
}

func completion(prompt string) []string {
	body := fmt.Sprintf("Answering %q. ", prompt) + strings.Repeat(
		"Streaming responses arrive one token at a time, which is what lets a chat interface render "+
			"text as it is produced rather than waiting for a whole reply. That same property is what "+
			"makes stopping useful: when the reader has seen enough, every token after that point is "+
			"wasted work on the server. ", 3)
	return strings.SplitAfter(body, " ")
}

func main() {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@127.0.0.1:25672/"
	}
	ctx := context.Background()
	bus, err := protobus.Dial(ctx, url)
	if err != nil {
		log.Fatal(err)
	}
	defer bus.Close()

	// A streaming handler holds its prefetch slot for the life of its stream,
	// so concurrency is how many callers are served at once.
	svc, err := chat.RegisterAssistantServer(bus, &assistant{}, protobus.WithMaxConcurrent(8))
	if err != nil {
		log.Fatal(err)
	}
	if err := svc.Start(ctx); err != nil {
		log.Fatal(err)
	}
	client := chat.NewAssistantClient(bus)

	stopButton(ctx, client)
	breakOut(ctx, client)
	runToCompletion(ctx, client)
}

// stopButton: Stop lives outside the loop, in another request handler, and
// takes effect at once rather than at the next token. In an HTTP server, pass
// the request's context and closing the tab stops the stream.
func stopButton(ctx context.Context, client chat.AssistantClient) {
	header("1. Stop button (context cancellation)")
	ctx, stop := context.WithCancel(ctx)
	time.AfterFunc(900*time.Millisecond, func() {
		fmt.Print("\n  [user pressed Stop]\n")
		stop()
	})
	fmt.Print("  ")
	for tok, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "why does streaming matter?", TokenDelayMs: 60}) {
		if errors.Is(err, context.Canceled) {
			break // the loop ends with the context's error: expected here
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(tok.Text)
	}
	report(ctx, client)
}

// breakOut: the decision is made inside the loop.
func breakOut(ctx context.Context, client chat.AssistantClient) {
	header("2. break out of the loop")
	fmt.Print("  ")
	printed := 0
	for tok, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "only the first few words", TokenDelayMs: 60}) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(tok.Text)
		if printed++; printed == 8 {
			break
		}
	}
	fmt.Print("\n  [consumer stopped reading]\n")
	report(ctx, client)
}

// runToCompletion: the control, where nothing cancels.
func runToCompletion(ctx context.Context, client chat.AssistantClient) {
	header("3. no cancellation")
	received := 0
	for _, err := range client.Generate(ctx, &chat.GenerateRequest{Prompt: "short answer", TokenDelayMs: 1}) {
		if err != nil {
			log.Fatal(err)
		}
		received++
	}
	fmt.Printf("  received %d tokens\n", received)
	report(ctx, client)
}

func report(ctx context.Context, client chat.AssistantClient) {
	time.Sleep(300 * time.Millisecond) // let the cancellation travel
	stats, err := client.Stats(context.WithoutCancel(ctx), &chat.StatsRequest{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  server generated %d tokens; stopped early: %v\n", stats.TokensGenerated, stats.StoppedEarly)
}

func header(title string) {
	fmt.Printf("\n%s\n%s\n%s\n", strings.Repeat("=", 64), title, strings.Repeat("=", 64))
}
