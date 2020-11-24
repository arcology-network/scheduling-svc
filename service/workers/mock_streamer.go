package workers

import (
	"fmt"
	"testing"

	"github.com/HPISTechnologies/component-lib/streamer"
)

type MockStreamer struct {
	t        testing.TB
	received map[string]interface{}
}

func NewMockStreamer(t testing.TB) *MockStreamer {
	return &MockStreamer{
		t:        t,
		received: make(map[string]interface{}),
	}
}

func (m *MockStreamer) Send(name string, data interface{}) {
	if _, ok := m.received[name]; ok {
		panic(fmt.Sprintf("Duplicate message received: %v", name))
	}

	m.received[name] = data
}

func (m *MockStreamer) RegisterProducer(p streamer.StreamProducer) {
	m.t.Logf(
		"MockStreamer.RegisterProducer:\nGetName(): %v\nGetOutputs(): %v\nGetBufferLength(): %v",
		p.GetName(), p.GetOutputs(), p.GetBufferLengths(),
	)
}

func (m *MockStreamer) RegisterConsumer(c streamer.StreamConsumer) {
	m.t.Logf(
		"MockStreamer.RegisterConsumer:\nGetName(): %v\nGetInputs(): %v",
		c.GetName(), c.GetInputs(),
	)
}

func (m *MockStreamer) GetReceivedData() map[string]interface{} {
	return m.received
}
