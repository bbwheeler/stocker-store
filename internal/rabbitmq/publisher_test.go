package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/events"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---- fakes ----

// fakeConfirmation is a confirmation test double with a scripted outcome. It
// records how often the publisher awaited it.
type fakeConfirmation struct {
	acked bool
	err   error

	mu    sync.Mutex
	waits int
}

func (c *fakeConfirmation) WaitContext(ctx context.Context) (bool, error) {
	c.mu.Lock()
	c.waits++
	c.mu.Unlock()
	if c.err != nil {
		return false, c.err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return c.acked, nil
}

func (c *fakeConfirmation) waitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waits
}

// pubAttempt scripts one PublishWithDeferredConfirmWithContext outcome.
type pubAttempt struct {
	publishErr error
	conf       *fakeConfirmation
}

// pubCall records one publish invocation on a fakePublishChannel.
type pubCall struct {
	exchange, key        string
	mandatory, immediate bool
	msg                  amqp.Publishing
}

// fakePublishChannel is a channel test double for the publisher. It records
// exchange declarations, Confirm calls, and publishes, and serves scripted
// publish outcomes in order.
type fakePublishChannel struct {
	attempts   []pubAttempt
	confirmErr error
	onPublish  func()

	mu            sync.Mutex
	exchangeDecls []exchangeDecl
	confirms      int
	publishes     []pubCall
	closed        bool
	closeErr      error
}

func (f *fakePublishChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exchangeDecls = append(f.exchangeDecls, exchangeDecl{name, kind, durable, autoDelete, internal, noWait, args})
	return nil
}

func (f *fakePublishChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{}, errors.New("queue declare not used by the publisher")
}

func (f *fakePublishChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	return errors.New("queue bind not used by the publisher")
}

func (f *fakePublishChannel) Qos(int, int, bool) error {
	return errors.New("qos not used by the publisher")
}

func (f *fakePublishChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, errors.New("consume not used by the publisher")
}

func (f *fakePublishChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error { return c }

func (f *fakePublishChannel) Confirm(bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirms++
	return f.confirmErr
}

func (f *fakePublishChannel) PublishWithDeferredConfirmWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error) {
	if f.onPublish != nil {
		f.onPublish()
	}
	f.mu.Lock()
	i := len(f.publishes)
	f.publishes = append(f.publishes, pubCall{exchange, key, mandatory, immediate, msg})
	f.mu.Unlock()
	if i >= len(f.attempts) {
		return nil, fmt.Errorf("unexpected publish attempt %d", i+1)
	}
	a := f.attempts[i]
	if a.publishErr != nil {
		return nil, a.publishErr
	}
	return a.conf, nil
}

func (f *fakePublishChannel) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakePublishChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakePublishChannel) markClosed() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakePublishChannel) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakePublishChannel) decls() []exchangeDecl {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]exchangeDecl(nil), f.exchangeDecls...)
}

func (f *fakePublishChannel) confirmCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.confirms
}

func (f *fakePublishChannel) sent() []pubCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pubCall(nil), f.publishes...)
}

// ---- helpers ----

func testPublisherConfig() Config {
	return Config{
		URL:                "amqp://guest:guest@broker:5672/",
		Exchange:           "stock.update",
		ExchangeType:       "topic",
		RoutingKey:         "stock.update.v1",
		Queue:              "stocker-store.stock.update.v1",
		Prefetch:           1,
		OutputExchange:     "stock.created",
		OutputExchangeType: "topic",
		OutputRoutingKey:   "stock.created.v1",
	}
}

// newTestPublisher builds an enabled publisher with an injected dialer so no
// test ever touches a broker.
func newTestPublisher(t *testing.T, d *fakeDialer) *Publisher {
	t.Helper()
	pub, err := NewPublisher(testPublisherConfig())
	if err != nil {
		t.Fatalf("NewPublisher() error = %v, want nil", err)
	}
	p, ok := pub.(*Publisher)
	if !ok {
		t.Fatalf("NewPublisher() = %T, want *Publisher", pub)
	}
	p.dial = d.dial
	return p
}

func testStock() *stockv1.Stock {
	return &stockv1.Stock{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores: []*stockv1.ScoreEntry{
			{Category: "momentum", Value: 0.5, UpdatedAt: timestamppb.New(time.Now())},
		},
	}
}

// ---- NewPublisher ----

func TestNewPublisher_DisabledReturnsNoopWithoutDialing(t *testing.T) {
	logBuf := captureLog(t)

	cfg := testPublisherConfig()
	cfg.OutputExchange = ""
	d := &fakeDialer{}

	pub, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v, want nil", err)
	}
	if _, ok := pub.(events.Noop); !ok {
		t.Errorf("NewPublisher() = %T, want events.Noop", pub)
	}
	if err := pub.Publish(context.Background(), testStock()); err != nil {
		t.Errorf("Publish() error = %v, want nil", err)
	}
	if err := pub.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 when output is disabled", got)
	}
	out := logBuf.String()
	want := "rabbitmq: output publisher disabled (RABBITMQ_OUTPUT_EXCHANGE not set)"
	if !strings.Contains(out, want) {
		t.Errorf("log output missing %q; got:\n%s", want, out)
	}
}

// TestNewPublisher_PerformsNoNetworkIO pins the lazy-dial contract: building
// the publisher validates the URL but never opens a connection.
func TestNewPublisher_PerformsNoNetworkIO(t *testing.T) {
	d := &fakeDialer{}
	newTestPublisher(t, d)
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 before the first publish", got)
	}
}

func TestNewPublisher_URLParseErrorRedactsPassword(t *testing.T) {
	cfg := testPublisherConfig()
	cfg.URL = "amqp://user:s3cret-password@host:5672/%zz"

	_, err := NewPublisher(cfg)
	if err == nil {
		t.Fatal("NewPublisher() error = nil, want a URL parse error")
	}
	msg := err.Error()
	if strings.Contains(msg, "s3cret-password") {
		t.Errorf("NewPublisher() error leaked the password: %q", msg)
	}
	if !strings.Contains(msg, "RABBITMQ_URL") {
		t.Errorf("NewPublisher() error = %q, want it to mention RABBITMQ_URL", msg)
	}
}

func TestNewPublisher_WarnsWhenOutputExchangeEqualsInputExchange(t *testing.T) {
	logBuf := captureLog(t)

	cfg := testPublisherConfig()
	cfg.OutputExchange = cfg.Exchange

	pub, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher() error = %v, want nil", err)
	}
	if _, ok := pub.(*Publisher); !ok {
		t.Fatalf("NewPublisher() = %T, want *Publisher", pub)
	}
	out := logBuf.String()
	want := "rabbitmq: output exchange equals input exchange; verify the routing key does not create a feedback loop"
	if !strings.Contains(out, want) {
		t.Errorf("log output missing %q; got:\n%s", want, out)
	}
}

// ---- Publish ----

// TestPublish_DeclaresExchangeAndSendsMessage pins the exchange flags, the
// mandatory/immediate flags, every message property, the protobuf body
// round-trip, and that confirms are enabled once per connection and awaited
// for each publish.
func TestPublish_DeclaresExchangeAndSendsMessage(t *testing.T) {
	cfg := testPublisherConfig()
	conf1 := &fakeConfirmation{acked: true}
	conf2 := &fakeConfirmation{acked: true}
	ch := &fakePublishChannel{attempts: []pubAttempt{{conf: conf1}, {conf: conf2}}}
	conn := &fakeConnection{ch: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newTestPublisher(t, d)

	stock := testStock()
	before := time.Now().UTC().Add(-time.Second)
	if err := p.Publish(context.Background(), stock); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	// A second publish on the same connection must not enable confirms again.
	if err := p.Publish(context.Background(), stock); err != nil {
		t.Fatalf("second Publish() error = %v, want nil", err)
	}
	after := time.Now().UTC().Add(time.Second)

	if got := d.callCount(); got != 1 {
		t.Errorf("dial calls = %d, want 1 (lazy dial, one connection)", got)
	}

	decls := ch.decls()
	if len(decls) != 1 {
		t.Fatalf("ExchangeDeclare calls = %d, want 1", len(decls))
	}
	ex := decls[0]
	if ex.name != cfg.OutputExchange || ex.kind != cfg.OutputExchangeType {
		t.Errorf("ExchangeDeclare(%q, %q), want (%q, %q)", ex.name, ex.kind, cfg.OutputExchange, cfg.OutputExchangeType)
	}
	if !ex.durable || ex.autoDelete || ex.internal || ex.noWait || ex.args != nil {
		t.Errorf("exchange flags = %+v, want durable=true autoDelete=false internal=false noWait=false args=nil", ex)
	}

	if got := ch.confirmCount(); got != 1 {
		t.Errorf("Confirm calls = %d, want 1 (once per connection)", got)
	}

	sent := ch.sent()
	if len(sent) != 2 {
		t.Fatalf("publish calls = %d, want 2", len(sent))
	}
	for i, call := range sent {
		if call.exchange != cfg.OutputExchange || call.key != cfg.OutputRoutingKey {
			t.Errorf("publish[%d] = (%q, %q), want (%q, %q)", i, call.exchange, call.key, cfg.OutputExchange, cfg.OutputRoutingKey)
		}
		if call.mandatory || call.immediate {
			t.Errorf("publish[%d] flags mandatory=%v immediate=%v, want false/false", i, call.mandatory, call.immediate)
		}
		msg := call.msg
		if msg.ContentType != "application/x-protobuf" {
			t.Errorf("publish[%d] ContentType = %q, want application/x-protobuf", i, msg.ContentType)
		}
		if msg.DeliveryMode != amqp.Persistent {
			t.Errorf("publish[%d] DeliveryMode = %d, want %d (persistent)", i, msg.DeliveryMode, amqp.Persistent)
		}
		if msg.MessageId != stock.Symbol {
			t.Errorf("publish[%d] MessageId = %q, want %q", i, msg.MessageId, stock.Symbol)
		}
		if msg.Type != cfg.OutputRoutingKey {
			t.Errorf("publish[%d] Type = %q, want %q", i, msg.Type, cfg.OutputRoutingKey)
		}
		if msg.Timestamp.Before(before) || msg.Timestamp.After(after) {
			t.Errorf("publish[%d] Timestamp = %v, want between %v and %v", i, msg.Timestamp, before, after)
		}
		if msg.Timestamp.Location() != time.UTC {
			t.Errorf("publish[%d] Timestamp location = %v, want UTC", i, msg.Timestamp.Location())
		}
		if got, _ := msg.Headers["symbol"].(string); got != stock.Symbol {
			t.Errorf("publish[%d] headers[symbol] = %v, want %q", i, msg.Headers["symbol"], stock.Symbol)
		}
		if got, _ := msg.Headers["exchange"].(string); got != stock.Exchange {
			t.Errorf("publish[%d] headers[exchange] = %v, want %q", i, msg.Headers["exchange"], stock.Exchange)
		}

		got := new(stockv1.Stock)
		if err := proto.Unmarshal(msg.Body, got); err != nil {
			t.Fatalf("publish[%d] body does not unmarshal: %v", i, err)
		}
		if !proto.Equal(got, stock) {
			t.Errorf("publish[%d] body = %v, want %v", i, got, stock)
		}
	}

	if got := conf1.waitCount(); got != 1 {
		t.Errorf("confirmation 1 waits = %d, want 1", got)
	}
	if got := conf2.waitCount(); got != 1 {
		t.Errorf("confirmation 2 waits = %d, want 1", got)
	}
}

// TestPublish_ConfirmNackedIsError pins that a negative confirmation fails the
// publish, tears down, reconnects, and is returned wrapped on the second
// failure.
func TestPublish_ConfirmNackedIsError(t *testing.T) {
	logBuf := captureLog(t)

	ch1 := &fakePublishChannel{attempts: []pubAttempt{
		{conf: &fakeConfirmation{acked: false}},
		{conf: &fakeConfirmation{acked: false}},
	}}
	conn1 := &fakeConnection{ch: ch1}
	ch2 := &fakePublishChannel{attempts: []pubAttempt{
		{conf: &fakeConfirmation{acked: false}},
	}}
	conn2 := &fakeConnection{ch: ch2}
	d := &fakeDialer{results: []dialResult{{conn: conn1}, {conn: conn2}}}
	p := newTestPublisher(t, d)

	err := p.Publish(context.Background(), testStock())
	if err == nil {
		t.Fatal("Publish() error = nil, want an error for acked=false")
	}
	want := `publish to rabbitmq exchange "stock.created"`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Publish() error = %q, want it to contain %q", err.Error(), want)
	}
	if !conn1.isClosed() {
		t.Error("first connection not closed after a failed publish")
	}
	out := logBuf.String()
	wantLog := "rabbitmq: publish failed; reconnecting and retrying once symbol=AAPL"
	if !strings.Contains(out, wantLog) {
		t.Errorf("log output missing %q; got:\n%s", wantLog, out)
	}
}

// TestPublish_RetriesOnceAfterFailure pins the reconnect-and-retry path: the
// first failure tears down, redials, and the retry of the same publishing
// value succeeds.
func TestPublish_RetriesOnceAfterFailure(t *testing.T) {
	logBuf := captureLog(t)

	ch1 := &fakePublishChannel{attempts: []pubAttempt{{publishErr: errors.New("connection reset")}}}
	conn1 := &fakeConnection{ch: ch1}
	conf2 := &fakeConfirmation{acked: true}
	ch2 := &fakePublishChannel{attempts: []pubAttempt{{conf: conf2}}}
	conn2 := &fakeConnection{ch: ch2}
	d := &fakeDialer{results: []dialResult{{conn: conn1}, {conn: conn2}}}
	p := newTestPublisher(t, d)

	stock := testStock()
	if err := p.Publish(context.Background(), stock); err != nil {
		t.Fatalf("Publish() error = %v, want nil after one retry", err)
	}
	if got := d.callCount(); got != 2 {
		t.Errorf("dial calls = %d, want 2 (redial after failure)", got)
	}
	if !conn1.isClosed() || !ch1.isClosed() {
		t.Error("first connection/channel not torn down after the failed attempt")
	}
	first := ch1.sent()
	second := ch2.sent()
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("publish calls = %d and %d, want 1 and 1", len(first), len(second))
	}
	if string(first[0].msg.Body) != string(second[0].msg.Body) {
		t.Error("retry did not reuse the same publishing body")
	}
	if first[0].msg.MessageId != second[0].msg.MessageId {
		t.Error("retry did not reuse the same publishing message id")
	}
	if got := conf2.waitCount(); got != 1 {
		t.Errorf("retry confirmation waits = %d, want 1", got)
	}
	out := logBuf.String()
	if !strings.Contains(out, "rabbitmq: publish failed; reconnecting and retrying once symbol=AAPL") {
		t.Errorf("log output missing the retry line; got:\n%s", out)
	}
}

// TestPublish_TwoConsecutiveFailuresReturnErrorAndStop pins that the retry is
// exactly one: a second failure is returned and no third attempt is made.
func TestPublish_TwoConsecutiveFailuresReturnErrorAndStop(t *testing.T) {
	logBuf := captureLog(t)

	ch1 := &fakePublishChannel{attempts: []pubAttempt{{publishErr: errors.New("boom")}}}
	conn1 := &fakeConnection{ch: ch1}
	ch2 := &fakePublishChannel{attempts: []pubAttempt{{publishErr: errors.New("boom again")}}}
	conn2 := &fakeConnection{ch: ch2}
	d := &fakeDialer{results: []dialResult{{conn: conn1}, {conn: conn2}}}
	p := newTestPublisher(t, d)

	err := p.Publish(context.Background(), testStock())
	if err == nil {
		t.Fatal("Publish() error = nil, want an error after two failures")
	}
	want := `publish to rabbitmq exchange "stock.created"`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Publish() error = %q, want it to contain %q", err.Error(), want)
	}
	if !strings.Contains(err.Error(), "boom again") {
		t.Errorf("Publish() error = %q, want it to wrap the second failure", err.Error())
	}
	if got := d.callCount(); got != 2 {
		t.Errorf("dial calls = %d, want 2 (no third attempt)", got)
	}
	if got := len(ch1.sent()) + len(ch2.sent()); got != 2 {
		t.Errorf("publish attempts = %d, want 2 (no third attempt)", got)
	}
	if !conn1.isClosed() || !conn2.isClosed() {
		t.Error("both connections should be closed after the final failure")
	}
	out := logBuf.String()
	if !strings.Contains(out, "rabbitmq: publish failed; reconnecting and retrying once symbol=AAPL") {
		t.Errorf("log output missing the retry line; got:\n%s", out)
	}
}

// TestPublish_ClosedChannelRedialsOnNextPublish pins that a channel the broker
// closed is detected and replaced by a fresh dial on the next publish.
func TestPublish_ClosedChannelRedialsOnNextPublish(t *testing.T) {
	ch1 := &fakePublishChannel{attempts: []pubAttempt{{conf: &fakeConfirmation{acked: true}}}}
	conn1 := &fakeConnection{ch: ch1}
	ch2 := &fakePublishChannel{attempts: []pubAttempt{{conf: &fakeConfirmation{acked: true}}}}
	conn2 := &fakeConnection{ch: ch2}
	d := &fakeDialer{results: []dialResult{{conn: conn1}, {conn: conn2}}}
	p := newTestPublisher(t, d)

	if err := p.Publish(context.Background(), testStock()); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	ch1.markClosed()

	if err := p.Publish(context.Background(), testStock()); err != nil {
		t.Fatalf("Publish() after channel close error = %v, want nil", err)
	}
	if got := d.callCount(); got != 2 {
		t.Errorf("dial calls = %d, want 2 (fresh dial on a closed channel)", got)
	}
	if got := len(ch2.sent()); got != 1 {
		t.Errorf("second channel publishes = %d, want 1", got)
	}
}

func TestPublish_NilStockReturnsErrorWithoutDialing(t *testing.T) {
	d := &fakeDialer{}
	p := newTestPublisher(t, d)

	if err := p.Publish(context.Background(), nil); err == nil {
		t.Fatal("Publish(nil) error = nil, want an error")
	}
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 for a nil stock", got)
	}
}

func TestPublish_AfterCloseReturnsError(t *testing.T) {
	ch := &fakePublishChannel{attempts: []pubAttempt{{conf: &fakeConfirmation{acked: true}}}}
	conn := &fakeConnection{ch: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newTestPublisher(t, d)

	if err := p.Publish(context.Background(), testStock()); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if !conn.isClosed() || !ch.isClosed() {
		t.Error("Close() did not close the channel and connection")
	}

	if err := p.Publish(context.Background(), testStock()); err == nil {
		t.Fatal("Publish() after Close error = nil, want an error")
	}
	if got := d.callCount(); got != 1 {
		t.Errorf("dial calls = %d, want 1 (no dial after Close)", got)
	}
	if got := len(ch.sent()); got != 1 {
		t.Errorf("publish attempts = %d, want 1 (no publish after Close)", got)
	}
}

func TestPublisher_CloseIsIdempotent(t *testing.T) {
	ch := &fakePublishChannel{attempts: []pubAttempt{{conf: &fakeConfirmation{acked: true}}}}
	conn := &fakeConnection{ch: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newTestPublisher(t, d)

	if err := p.Publish(context.Background(), testStock()); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil (idempotent)", err)
	}
	// Close without ever publishing must also be safe.
	if err := (&Publisher{exchange: "x", exchangeType: "topic"}).Close(); err != nil {
		t.Errorf("Close() on a fresh publisher error = %v, want nil", err)
	}
}

func TestPublish_CancelledContextSkipsDial(t *testing.T) {
	d := &fakeDialer{}
	p := newTestPublisher(t, d)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.Publish(ctx, testStock())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context.Canceled", err)
	}
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 for a cancelled context", got)
	}
}

// TestPublish_CancelledContextSkipsRetry pins that a failure racing with
// cancellation returns immediately instead of reconnecting and retrying.
func TestPublish_CancelledContextSkipsRetry(t *testing.T) {
	logBuf := captureLog(t)

	ctx, cancel := context.WithCancel(context.Background())
	ch := &fakePublishChannel{
		attempts:  []pubAttempt{{publishErr: errors.New("connection reset")}},
		onPublish: cancel,
	}
	conn := &fakeConnection{ch: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}
	p := newTestPublisher(t, d)

	err := p.Publish(ctx, testStock())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context.Canceled", err)
	}
	if got := d.callCount(); got != 1 {
		t.Errorf("dial calls = %d, want 1 (no retry after cancellation)", got)
	}
	if got := len(ch.sent()); got != 1 {
		t.Errorf("publish attempts = %d, want 1 (no retry after cancellation)", got)
	}
	out := logBuf.String()
	if strings.Contains(out, "reconnecting and retrying once") {
		t.Errorf("cancelled publish must not log a retry; got:\n%s", out)
	}
}

var _ events.Publisher = (*Publisher)(nil)
