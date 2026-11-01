package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"git.wheeli.ca/brian/stocker-store/internal/events"
	"git.wheeli.ca/brian/stocker-store/internal/store"
	stockv1 "git.wheeli.ca/brian/stocker-store/proto/v1"

	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---- fakes ----

// eventLog is a mutex-protected ordered event log used to assert the relative
// order of store writes and acknowledgements.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) record(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// scriptedStore is a Store test double. It records every call, can fail the
// first failFirst calls with err, and reports the configured inserted flag.
type scriptedStore struct {
	events    *eventLog
	failFirst int
	err       error
	inserted  bool

	mu         sync.Mutex
	calls      int
	lastSymbol string
	lastExch   string
	lastScores map[string]float64
}

func (s *scriptedStore) UpdateStock(_ context.Context, symbol, exchange string, scores map[string]float64) (*store.Stock, bool, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.lastSymbol = symbol
	s.lastExch = exchange
	s.lastScores = scores
	inserted := s.inserted
	s.mu.Unlock()

	if s.events != nil {
		s.events.record("update")
	}
	if n <= s.failFirst {
		return nil, false, s.err
	}
	return &store.Stock{
		Symbol:   symbol,
		Exchange: exchange,
		Updated:  time.Now(),
		Scores:   toStoreScores(scores),
	}, inserted, nil
}

func (s *scriptedStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedStore) last() (symbol, exchange string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSymbol, s.lastExch
}

func (s *scriptedStore) scores() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastScores
}

// toStoreScores expands a category->value map into the slice the production
// store returns.
func toStoreScores(m map[string]float64) []store.ScoreEntry {
	if m == nil {
		return nil
	}
	out := make([]store.ScoreEntry, 0, len(m))
	for cat, v := range m {
		out = append(out, store.ScoreEntry{Category: cat, Value: v, UpdatedAt: time.Now()})
	}
	return out
}

// fakePublisher is an events.Publisher test double.
type fakePublisher struct {
	mu    sync.Mutex
	calls int
	last  *stockv1.Stock
	err   error
}

func (f *fakePublisher) Publish(_ context.Context, stock *stockv1.Stock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = stock
	return f.err
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ events.Publisher = (*fakePublisher)(nil)

// fakeAcknowledger records ack and nack calls on an amqp.Delivery.
type fakeAcknowledger struct {
	events *eventLog
	ackErr error
	nakErr error

	mu      sync.Mutex
	acks    int
	nacks   int
	requeue []bool
}

func (a *fakeAcknowledger) Ack(uint64, bool) error {
	a.mu.Lock()
	a.acks++
	a.mu.Unlock()
	if a.events != nil {
		a.events.record("ack")
	}
	return a.ackErr
}

func (a *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.mu.Lock()
	a.nacks++
	a.requeue = append(a.requeue, requeue)
	a.mu.Unlock()
	if a.events != nil {
		a.events.record("nack")
	}
	return a.nakErr
}

func (a *fakeAcknowledger) Reject(uint64, bool) error { return nil }

func (a *fakeAcknowledger) counts() (acks, nacks int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acks, a.nacks
}

func newAcknowledger() *fakeAcknowledger { return &fakeAcknowledger{} }

// ---- AMQP fakes ----

type exchangeDecl struct {
	name, kind                            string
	durable, autoDelete, internal, noWait bool
	args                                  amqp.Table
}

type queueDecl struct {
	name                                   string
	durable, autoDelete, exclusive, noWait bool
	args                                   amqp.Table
}

type bindCall struct {
	name, key, exchange string
	noWait              bool
	args                amqp.Table
}

type qosCall struct {
	prefetchCount, prefetchSize int
	global                      bool
}

type consumeCall struct {
	queue, consumer                     string
	autoAck, exclusive, noLocal, noWait bool
	args                                amqp.Table
}

// fakeChannel is a channel test double. It records topology calls and serves
// deliveries and close notifications from test-owned channels.
type fakeChannel struct {
	deliveries chan amqp.Delivery
	notify     chan *amqp.Error
	consumed   chan struct{}

	exchangeErr error
	queueErr    error
	bindErr     error
	qosErr      error
	consumeErr  error

	mu            sync.Mutex
	exchangeDecls []exchangeDecl
	queueDecls    []queueDecl
	binds         []bindCall
	qosCalls      []qosCall
	consumes      []consumeCall
	closed        bool
	closeErr      error
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	f.exchangeDecls = append(f.exchangeDecls, exchangeDecl{name, kind, durable, autoDelete, internal, noWait, args})
	f.mu.Unlock()
	return f.exchangeErr
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	f.queueDecls = append(f.queueDecls, queueDecl{name, durable, autoDelete, exclusive, noWait, args})
	f.mu.Unlock()
	return amqp.Queue{Name: name}, f.queueErr
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	f.binds = append(f.binds, bindCall{name, key, exchange, noWait, args})
	f.mu.Unlock()
	return f.bindErr
}

func (f *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.mu.Lock()
	f.qosCalls = append(f.qosCalls, qosCall{prefetchCount, prefetchSize, global})
	f.mu.Unlock()
	return f.qosErr
}

func (f *fakeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	f.consumes = append(f.consumes, consumeCall{queue, consumer, autoAck, exclusive, noLocal, noWait, args})
	f.mu.Unlock()
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	if f.consumed != nil {
		select {
		case <-f.consumed:
		default:
			close(f.consumed)
		}
	}
	return f.deliveries, nil
}

func (f *fakeChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.notify == nil {
		f.notify = c
	}
	return f.notify
}

func (f *fakeChannel) Confirm(bool) error { return nil }

func (f *fakeChannel) PublishWithDeferredConfirmWithContext(context.Context, string, string, bool, bool, amqp.Publishing) (confirmation, error) {
	return nil, errors.New("publish not used by the consumer")
}

func (f *fakeChannel) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakeChannel) exchange() []exchangeDecl {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]exchangeDecl(nil), f.exchangeDecls...)
}

func (f *fakeChannel) queues() []queueDecl {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queueDecl(nil), f.queueDecls...)
}

func (f *fakeChannel) bindings() []bindCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bindCall(nil), f.binds...)
}

func (f *fakeChannel) qos() []qosCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]qosCall(nil), f.qosCalls...)
}

func (f *fakeChannel) consumerCalls() []consumeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]consumeCall(nil), f.consumes...)
}

func (f *fakeChannel) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// fakeConnection is a connection test double.
type fakeConnection struct {
	ch    channel
	chErr error

	mu       sync.Mutex
	closed   bool
	closeErr error
}

func (c *fakeConnection) Channel() (channel, error) {
	if c.chErr != nil {
		return nil, c.chErr
	}
	return c.ch, nil
}

func (c *fakeConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return c.closeErr
}

func (c *fakeConnection) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// dialResult is one scripted dial outcome.
type dialResult struct {
	conn connection
	err  error
}

// fakeDialer returns scripted dial outcomes in order and records call URLs.
type fakeDialer struct {
	results []dialResult

	mu    sync.Mutex
	calls int
	urls  []string
}

func (d *fakeDialer) dial(url string) (connection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	i := d.calls
	d.calls++
	d.urls = append(d.urls, url)
	if i >= len(d.results) {
		return nil, fmt.Errorf("unexpected dial %d", i+1)
	}
	return d.results[i].conn, d.results[i].err
}

func (d *fakeDialer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// ---- helpers ----

func testConsumerConfig() Config {
	return Config{
		URL:          "amqp://guest:guest@broker:5672/",
		Exchange:     "stock.update",
		ExchangeType: "topic",
		RoutingKey:   "stock.update.v1",
		Queue:        "stocker-store.stock.update.v1",
		Prefetch:     5,
	}
}

func testDelivery(ack amqp.Acknowledger, body []byte, symbol, exchange, messageID string) amqp.Delivery {
	headers := amqp.Table{}
	if symbol != "" {
		headers["symbol"] = symbol
	}
	if exchange != "" {
		headers["exchange"] = exchange
	}
	return amqp.Delivery{
		Acknowledger: ack,
		Headers:      headers,
		MessageId:    messageID,
		Body:         body,
	}
}

func marshalStock(t *testing.T, symbol, exchange string, scores map[string]float64) []byte {
	t.Helper()
	var entries []*stockv1.ScoreEntry
	for cat, v := range scores {
		entries = append(entries, &stockv1.ScoreEntry{
			Category:  cat,
			Value:     v,
			UpdatedAt: timestamppb.New(time.Now()),
		})
	}
	raw, err := proto.Marshal(&stockv1.Stock{Symbol: symbol, Exchange: exchange, Scores: entries})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	return raw
}

// captureLog redirects the standard logger into a buffer for the duration of
// the test. Tests using it must not run in parallel (the logger is global).
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return &buf
}

// waitFor polls cond until it becomes true, failing the test after 2 s.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not satisfied within 2s")
}

func wantEvents(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("events[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

// ---- consumer topology / lifecycle ----

func TestConsumeOnce_DeclaresTopology(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	consumed := make(chan struct{})
	ch := &fakeChannel{deliveries: deliveries, consumed: consumed}
	conn := &fakeConnection{ch: ch}
	cfg := testConsumerConfig()

	c := New(cfg, &scriptedStore{}, nil)
	c.dial = func(string) (connection, error) { return conn, nil }

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		connected bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		connected, err := c.consumeOnce(ctx)
		done <- result{connected, err}
	}()

	<-consumed
	cancel()

	var res result
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumeOnce did not return after cancellation")
	}
	if res.err != nil {
		t.Fatalf("consumeOnce() error = %v, want nil", res.err)
	}
	if !res.connected {
		t.Error("consumeOnce() connected = false, want true")
	}

	exchanges := ch.exchange()
	if len(exchanges) != 1 {
		t.Fatalf("ExchangeDeclare calls = %d, want 1", len(exchanges))
	}
	ex := exchanges[0]
	if ex.name != cfg.Exchange || ex.kind != cfg.ExchangeType {
		t.Errorf("ExchangeDeclare(%q, %q), want (%q, %q)", ex.name, ex.kind, cfg.Exchange, cfg.ExchangeType)
	}
	if !ex.durable || ex.autoDelete || ex.internal || ex.noWait || ex.args != nil {
		t.Errorf("exchange flags = %+v, want durable=true autoDelete=false internal=false noWait=false args=nil", ex)
	}

	queues := ch.queues()
	if len(queues) != 1 {
		t.Fatalf("QueueDeclare calls = %d, want 1", len(queues))
	}
	q := queues[0]
	if q.name != cfg.Queue {
		t.Errorf("QueueDeclare(%q), want %q", q.name, cfg.Queue)
	}
	if !q.durable || q.autoDelete || q.exclusive || q.noWait || q.args != nil {
		t.Errorf("queue flags = %+v, want durable=true autoDelete=false exclusive=false noWait=false args=nil", q)
	}

	binds := ch.bindings()
	if len(binds) != 1 {
		t.Fatalf("QueueBind calls = %d, want 1", len(binds))
	}
	b := binds[0]
	if b.name != cfg.Queue || b.key != cfg.RoutingKey || b.exchange != cfg.Exchange || b.noWait || b.args != nil {
		t.Errorf("QueueBind(%q, %q, %q, %v, %v), want (%q, %q, %q, false, nil)",
			b.name, b.key, b.exchange, b.noWait, b.args, cfg.Queue, cfg.RoutingKey, cfg.Exchange)
	}

	qos := ch.qos()
	if len(qos) != 1 {
		t.Fatalf("Qos calls = %d, want 1", len(qos))
	}
	if qos[0] != (qosCall{cfg.Prefetch, 0, false}) {
		t.Errorf("Qos(%d, %d, %v), want (%d, 0, false)", qos[0].prefetchCount, qos[0].prefetchSize, qos[0].global, cfg.Prefetch)
	}

	consumes := ch.consumerCalls()
	if len(consumes) != 1 {
		t.Fatalf("Consume calls = %d, want 1", len(consumes))
	}
	cons := consumes[0]
	if cons.queue != cfg.Queue || cons.consumer != "" || cons.autoAck || cons.exclusive || cons.noLocal || cons.noWait || cons.args != nil {
		t.Errorf("Consume call = %+v, want (%q, \"\", false, false, false, false, nil)", cons, cfg.Queue)
	}

	if !conn.isClosed() {
		t.Error("connection not closed after cancellation")
	}
	if !ch.isClosed() {
		t.Error("channel not closed after cancellation")
	}
}

func TestConsumeOnce_NotifyCloseReturnsError(t *testing.T) {
	notify := make(chan *amqp.Error, 1)
	notify <- &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"}
	ch := &fakeChannel{deliveries: make(chan amqp.Delivery), notify: notify}
	conn := &fakeConnection{ch: ch}

	c := New(testConsumerConfig(), &scriptedStore{}, nil)
	c.dial = func(string) (connection, error) { return conn, nil }

	connected, err := c.consumeOnce(context.Background())
	if !connected {
		t.Error("consumeOnce() connected = false, want true")
	}
	if err == nil {
		t.Fatal("consumeOnce() error = nil, want a channel-closed error")
	}
	if !strings.Contains(err.Error(), "channel closed") {
		t.Errorf("consumeOnce() error = %q, want it to contain %q", err.Error(), "channel closed")
	}
}

func TestConsumeOnce_SetupErrorIsNotConnected(t *testing.T) {
	ch := &fakeChannel{
		deliveries:  make(chan amqp.Delivery),
		exchangeErr: errors.New("exchange declare failed"),
	}
	conn := &fakeConnection{ch: ch}

	c := New(testConsumerConfig(), &scriptedStore{}, nil)
	c.dial = func(string) (connection, error) { return conn, nil }

	connected, err := c.consumeOnce(context.Background())
	if connected {
		t.Error("consumeOnce() connected = true, want false for a setup failure")
	}
	if err == nil {
		t.Fatal("consumeOnce() error = nil, want the declare error")
	}
	if !conn.isClosed() || !ch.isClosed() {
		t.Error("connection/channel not closed after a setup failure")
	}
}

// TestRun_DisabledReturnsNilWithoutDialing pins that an explicitly empty URL
// disables ingestion without touching the broker.
func TestRun_DisabledReturnsNilWithoutDialing(t *testing.T) {
	cfg := testConsumerConfig()
	cfg.URL = ""
	d := &fakeDialer{}
	c := New(cfg, &scriptedStore{}, nil)
	c.dial = d.dial

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 when ingestion is disabled", got)
	}
}

func TestRun_ReturnsConfigValidationError(t *testing.T) {
	cfg := testConsumerConfig()
	cfg.ExchangeType = "queue"
	d := &fakeDialer{}
	c := New(cfg, &scriptedStore{}, nil)
	c.dial = d.dial

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want a config validation error")
	}
	if !strings.Contains(err.Error(), "RABBITMQ_EXCHANGE_TYPE") {
		t.Errorf("Run() error = %q, want it to mention RABBITMQ_EXCHANGE_TYPE", err.Error())
	}
	if got := d.callCount(); got != 0 {
		t.Errorf("dial calls = %d, want 0 for invalid configuration", got)
	}
}

// TestRun_ConnectFailureRetriesWithBackoff verifies a dial failure logs the
// connect-failed line and reconnects instead of terminating the process.
func TestRun_ConnectFailureRetriesWithBackoff(t *testing.T) {
	logBuf := captureLog(t)

	d := &fakeDialer{results: []dialResult{{err: errors.New("connection refused")}}}
	c := New(testConsumerConfig(), &scriptedStore{}, nil)
	c.dial = d.dial

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.sleep = func(context.Context, time.Duration) bool {
		cancel()
		return false
	}

	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if got := d.callCount(); got != 1 {
		t.Errorf("dial calls = %d, want 1", got)
	}
	out := logBuf.String()
	for _, want := range []string{
		"rabbitmq: connect failed: connection refused; retrying in 1s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got:\n%s", want, out)
		}
	}
}

// TestRun_RedialsAfterDeliveryChannelClose verifies a closed delivery channel
// tears down the session, logs the reconnect line, and dials again.
func TestRun_RedialsAfterDeliveryChannelClose(t *testing.T) {
	logBuf := captureLog(t)

	firstDeliveries := make(chan amqp.Delivery)
	close(firstDeliveries)
	firstCh := &fakeChannel{deliveries: firstDeliveries}
	firstConn := &fakeConnection{ch: firstCh}

	secondDeliveries := make(chan amqp.Delivery, 1)
	secondCh := &fakeChannel{deliveries: secondDeliveries}
	secondConn := &fakeConnection{ch: secondCh}
	secondDeliveries <- testDelivery(newAcknowledger(),
		marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
		"AAPL", "NASDAQ", "AAPL")

	d := &fakeDialer{results: []dialResult{{conn: firstConn}, {conn: secondConn}}}
	st := &scriptedStore{}
	c := New(testConsumerConfig(), st, nil)
	c.dial = d.dial
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return st.callCount() == 1 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if got := d.callCount(); got < 2 {
		t.Errorf("dial calls = %d, want at least 2 (redial after channel close)", got)
	}
	if sym, exch := st.last(); sym != "AAPL" || exch != "NASDAQ" {
		t.Errorf("store got %q/%q, want AAPL/NASDAQ", sym, exch)
	}
	out := logBuf.String()
	for _, want := range []string{
		"rabbitmq: consumer stopped",
		"reconnecting in",
		"rabbitmq: connected",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got:\n%s", want, out)
		}
	}
}

// TestRun_ClosesResourcesOnCancellation verifies graceful shutdown closes the
// channel and connection and logs the connected line.
func TestRun_ClosesResourcesOnCancellation(t *testing.T) {
	logBuf := captureLog(t)

	consumed := make(chan struct{})
	ch := &fakeChannel{deliveries: make(chan amqp.Delivery), consumed: consumed}
	conn := &fakeConnection{ch: ch}
	d := &fakeDialer{results: []dialResult{{conn: conn}}}

	c := New(testConsumerConfig(), &scriptedStore{}, nil)
	c.dial = d.dial
	c.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-consumed
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if !conn.isClosed() {
		t.Error("connection not closed on cancellation")
	}
	if !ch.isClosed() {
		t.Error("channel not closed on cancellation")
	}
	out := logBuf.String()
	if !strings.Contains(out, `rabbitmq: connected exchange="stock.update" type="topic" queue="stocker-store.stock.update.v1" routing_key="stock.update.v1"`) {
		t.Errorf("log output missing the connected line; got:\n%s", out)
	}
}

// ---- delivery semantics ----

func TestProcessDelivery_AcksAfterStoreWrite(t *testing.T) {
	logBuf := captureLog(t)

	events := &eventLog{}
	ack := &fakeAcknowledger{events: events}
	st := &scriptedStore{events: events}
	c := New(testConsumerConfig(), st, nil)
	d := testDelivery(ack,
		marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
		"AAPL", "NASDAQ", "AAPL")

	if err := c.processDelivery(context.Background(), d); err != nil {
		t.Fatalf("processDelivery() error = %v, want nil", err)
	}
	wantEvents(t, events.snapshot(), []string{"update", "ack"})
	if acks, nacks := ack.counts(); acks != 1 || nacks != 0 {
		t.Errorf("acks/nacks = %d/%d, want 1/0", acks, nacks)
	}
	if st.callCount() != 1 {
		t.Errorf("store calls = %d, want 1", st.callCount())
	}
	out := logBuf.String()
	for _, want := range []string{
		"rabbitmq: applied symbol=AAPL exchange=NASDAQ scores=1 inserted=false",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got:\n%s", want, out)
		}
	}
}

func TestProcessDelivery_RetriesTransientWithCappedBackoff(t *testing.T) {
	logBuf := captureLog(t)

	events := &eventLog{}
	ack := &fakeAcknowledger{events: events}
	st := &scriptedStore{events: events, failFirst: 6, err: errors.New("connection refused")}
	c := New(testConsumerConfig(), st, nil)

	var delays []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) bool {
		delays = append(delays, d)
		return true
	}
	d := testDelivery(ack,
		marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
		"AAPL", "NASDAQ", "AAPL")

	if err := c.processDelivery(context.Background(), d); err != nil {
		t.Fatalf("processDelivery() error = %v, want nil", err)
	}

	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second,
	}
	if len(delays) != len(want) {
		t.Fatalf("retry delays = %v, want %v", delays, want)
	}
	for i := range want {
		if delays[i] != want[i] {
			t.Errorf("retry delay[%d] = %v, want %v", i, delays[i], want[i])
		}
	}
	if st.callCount() != 7 {
		t.Errorf("store calls = %d, want 7 (one initial attempt plus six retries)", st.callCount())
	}
	if acks, nacks := ack.counts(); acks != 1 || nacks != 0 {
		t.Errorf("acks/nacks = %d/%d, want 1/0", acks, nacks)
	}
	out := logBuf.String()
	for _, wantLog := range []string{
		"rabbitmq: transient failure for symbol=AAPL/NASDAQ, retrying in 1s: connection refused",
		"rabbitmq: applied symbol=AAPL exchange=NASDAQ",
	} {
		if !strings.Contains(out, wantLog) {
			t.Errorf("log output missing %q; got:\n%s", wantLog, out)
		}
	}
}

func TestProcessDelivery_PermanentFailuresNackWithoutRetry(t *testing.T) {
	testCases := []struct {
		name      string
		body      []byte
		symbol    string
		exchange  string
		messageID string
	}{
		{
			name:      "undecodable payload",
			body:      []byte{0x01, 0x02, 0xff, 0xfe},
			symbol:    "corrupt",
			exchange:  "TEST",
			messageID: "corrupt",
		},
		{
			name:      "missing symbol",
			body:      marshalStock(t, "", "NASDAQ", nil),
			symbol:    "",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "missing exchange",
			body:      marshalStock(t, "AAPL", "", nil),
			symbol:    "AAPL",
			exchange:  "",
			messageID: "AAPL",
		},
		{
			name:      "missing both",
			body:      marshalStock(t, "", "", nil),
			symbol:    "",
			exchange:  "",
			messageID: "AAPL",
		},
		{
			name:      "nan score",
			body:      marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"sentiment": math.NaN()}),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "positive infinity score",
			body:      marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"quality": math.Inf(1)}),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "negative infinity score",
			body:      marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"quality": math.Inf(-1)}),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "score too high",
			body:      marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 1.5}),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "score too low",
			body:      marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"value": -2}),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
		{
			name:      "pg integrity violation",
			body:      marshalStock(t, "AAPL", "NASDAQ", nil),
			symbol:    "AAPL",
			exchange:  "NASDAQ",
			messageID: "AAPL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logBuf := captureLog(t)

			st := &scriptedStore{}
			if tc.name == "pg integrity violation" {
				st.failFirst = 1
				st.err = &pgconn.PgError{Code: "23514", Message: "violates check constraint"}
			}
			ack := newAcknowledger()
			c := New(testConsumerConfig(), st, nil)
			c.sleep = func(context.Context, time.Duration) bool {
				t.Error("sleep called for a permanent failure; it must not be retried")
				return true
			}

			d := testDelivery(ack, tc.body, tc.symbol, tc.exchange, tc.messageID)
			if err := c.processDelivery(context.Background(), d); err != nil {
				t.Fatalf("processDelivery() error = %v, want nil", err)
			}

			acks, nacks := ack.counts()
			if acks != 0 || nacks != 1 {
				t.Errorf("acks/nacks = %d/%d, want 0/1", acks, nacks)
			}
			if len(ack.requeue) != 1 || ack.requeue[0] {
				t.Errorf("nack requeue flags = %v, want [false]", ack.requeue)
			}
			if tc.name != "pg integrity violation" && st.callCount() != 0 {
				t.Errorf("store calls = %d, want 0 for an invalid message", st.callCount())
			}
			out := logBuf.String()
			// The symbol falls back to the message id when the header is absent.
			logSymbol := tc.symbol
			if logSymbol == "" {
				logSymbol = tc.messageID
			}
			wantLog := fmt.Sprintf("rabbitmq: skipping message symbol=%s exchange=%s message_id=%s:", logSymbol, tc.exchange, tc.messageID)
			if !strings.Contains(out, wantLog) {
				t.Errorf("log output missing %q; got:\n%s", wantLog, out)
			}
		})
	}
}

func TestProcessDelivery_ScorelessMessagePassesNilMapAndAcks(t *testing.T) {
	st := &scriptedStore{}
	ack := newAcknowledger()
	c := New(testConsumerConfig(), st, nil)
	d := testDelivery(ack, marshalStock(t, "AAPL", "NASDAQ", nil), "AAPL", "NASDAQ", "AAPL")

	if err := c.processDelivery(context.Background(), d); err != nil {
		t.Fatalf("processDelivery() error = %v, want nil", err)
	}
	if got := st.scores(); got != nil {
		t.Errorf("store scores = %v, want nil", got)
	}
	if acks, nacks := ack.counts(); acks != 1 || nacks != 0 {
		t.Errorf("acks/nacks = %d/%d, want 1/0", acks, nacks)
	}
}

func TestProcessDelivery_ContextCancellationDuringRetryDoesNotAck(t *testing.T) {
	st := &scriptedStore{failFirst: 100, err: errors.New("connection refused")}
	ack := newAcknowledger()
	c := New(testConsumerConfig(), st, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.sleep = func(context.Context, time.Duration) bool {
		cancel()
		return false
	}
	d := testDelivery(ack, marshalStock(t, "AAPL", "NASDAQ", nil), "AAPL", "NASDAQ", "AAPL")

	err := c.processDelivery(ctx, d)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processDelivery() error = %v, want context.Canceled", err)
	}
	if acks, nacks := ack.counts(); acks != 0 || nacks != 0 {
		t.Errorf("acks/nacks = %d/%d, want 0/0 (cancellation must not ack or nack)", acks, nacks)
	}
}

func TestProcessDelivery_PublishesOnlyOnInsert(t *testing.T) {
	testCases := []struct {
		name     string
		inserted bool
		want     int
	}{
		{"insert", true, 1},
		{"update", false, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := &scriptedStore{inserted: tc.inserted}
			pub := &fakePublisher{}
			ack := newAcknowledger()
			c := New(testConsumerConfig(), st, pub)
			d := testDelivery(ack,
				marshalStock(t, "AAPL", "NASDAQ", map[string]float64{"momentum": 0.5}),
				"AAPL", "NASDAQ", "AAPL")

			if err := c.processDelivery(context.Background(), d); err != nil {
				t.Fatalf("processDelivery() error = %v, want nil", err)
			}
			if got := pub.callCount(); got != tc.want {
				t.Errorf("publisher calls = %d, want %d", got, tc.want)
			}
			if tc.inserted {
				if got := pub.last.GetSymbol(); got != "AAPL" {
					t.Errorf("published symbol = %q, want AAPL", got)
				}
				if got := pub.last.GetExchange(); got != "NASDAQ" {
					t.Errorf("published exchange = %q, want NASDAQ", got)
				}
			}
			if acks, _ := ack.counts(); acks != 1 {
				t.Errorf("acks = %d, want 1", acks)
			}
		})
	}
}

func TestProcessDelivery_PublishFailureIsLoggedAndAcked(t *testing.T) {
	logBuf := captureLog(t)

	st := &scriptedStore{inserted: true}
	pub := &fakePublisher{err: errors.New("output exchange unavailable")}
	ack := newAcknowledger()
	c := New(testConsumerConfig(), st, pub)
	d := testDelivery(ack, marshalStock(t, "AAPL", "NASDAQ", nil), "AAPL", "NASDAQ", "AAPL")

	if err := c.processDelivery(context.Background(), d); err != nil {
		t.Fatalf("processDelivery() error = %v, want nil (publish is best-effort)", err)
	}
	if acks, nacks := ack.counts(); acks != 1 || nacks != 0 {
		t.Errorf("acks/nacks = %d/%d, want 1/0", acks, nacks)
	}
	out := logBuf.String()
	for _, want := range []string{
		"rabbitmq: failed to publish new stock AAPL/NASDAQ: output exchange unavailable",
		"rabbitmq: applied symbol=AAPL exchange=NASDAQ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; got:\n%s", want, out)
		}
	}
}

func TestProcessDelivery_AckFailureReturnsError(t *testing.T) {
	st := &scriptedStore{}
	ack := &fakeAcknowledger{ackErr: errors.New("channel closed")}
	c := New(testConsumerConfig(), st, nil)
	d := testDelivery(ack, marshalStock(t, "AAPL", "NASDAQ", nil), "AAPL", "NASDAQ", "AAPL")

	err := c.processDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("processDelivery() error = nil, want an ack error")
	}
	if !strings.Contains(err.Error(), "ack message") {
		t.Errorf("processDelivery() error = %q, want it to mention the ack", err.Error())
	}
}

func TestProcessDelivery_NackFailureReturnsError(t *testing.T) {
	st := &scriptedStore{}
	ack := &fakeAcknowledger{nakErr: errors.New("channel closed")}
	c := New(testConsumerConfig(), st, nil)
	d := testDelivery(ack, []byte{0x01, 0xff, 0xfe}, "corrupt", "TEST", "corrupt")

	err := c.processDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("processDelivery() error = nil, want a nack error")
	}
	if !strings.Contains(err.Error(), "nack message") {
		t.Errorf("processDelivery() error = %q, want it to mention the nack", err.Error())
	}
	if st.callCount() != 0 {
		t.Errorf("store calls = %d, want 0", st.callCount())
	}
}

func TestIsPermanent(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{"invalid message sentinel", fmt.Errorf("%w: bad payload", errInvalidMessage), true},
		{"wrapped invalid message", fmt.Errorf("outer: %w", fmt.Errorf("%w: bad", errInvalidMessage)), true},
		{"pg check violation 23514", &pgconn.PgError{Code: "23514"}, true},
		{"pg fk violation 23503", &pgconn.PgError{Code: "23503"}, true},
		{"pg bad input 22P02", &pgconn.PgError{Code: "22P02"}, true},
		{"wrapped pg error", fmt.Errorf("upsert score: %w", &pgconn.PgError{Code: "23514"}), true},
		{"pg serialization failure 40001", &pgconn.PgError{Code: "40001"}, false},
		{"pg error with empty code", &pgconn.PgError{}, false},
		{"plain error", errors.New("connection refused"), false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPermanent(tc.err); got != tc.want {
				t.Errorf("isPermanent(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// ---- decode helpers ----

func TestDecodeStock_RoundTrip(t *testing.T) {
	scores := map[string]float64{"momentum": 0.5, "value": -0.25}
	raw := marshalStock(t, "AAPL", "NASDAQ", scores)

	m, err := decodeStock(raw)
	if err != nil {
		t.Fatalf("decodeStock() error = %v", err)
	}
	if m.GetSymbol() != "AAPL" || m.GetExchange() != "NASDAQ" {
		t.Errorf("decoded = %q/%q, want AAPL/NASDAQ", m.GetSymbol(), m.GetExchange())
	}
	if len(m.GetScores()) != len(scores) {
		t.Fatalf("scores = %d, want %d", len(m.GetScores()), len(scores))
	}
}

func TestDecodeStock_ScoresAbsent(t *testing.T) {
	raw := marshalStock(t, "AAPL", "NASDAQ", nil)
	m, err := decodeStock(raw)
	if err != nil {
		t.Fatalf("decodeStock() error = %v", err)
	}
	if m.GetScores() != nil {
		t.Errorf("scores = %v, want nil", m.GetScores())
	}
}

func TestDecodeStock_BadProto(t *testing.T) {
	_, err := decodeStock([]byte{0x01, 0x02, 0xff, 0xfe})
	if err == nil {
		t.Fatal("decodeStock() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "decode stock message") {
		t.Errorf("decodeStock() error = %q, want it to contain %q", err.Error(), "decode stock message")
	}
}

func TestNew_DefaultPublisherIsNoop(t *testing.T) {
	c := New(testConsumerConfig(), &scriptedStore{}, nil)
	if _, ok := c.publisher.(events.Noop); !ok {
		t.Errorf("publisher = %T, want events.Noop", c.publisher)
	}
}
