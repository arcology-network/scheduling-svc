package workers

import (
	"fmt"
	"testing"

	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
)

type MockArbitrator struct {
	t         *testing.T
	results   [][]*ethCommon.Hash
	nextIndex int
}

func NewMockArbitrator(t *testing.T, results [][]*ethCommon.Hash) *MockArbitrator {
	return &MockArbitrator{
		t:       t,
		results: results,
	}
}

func (m *MockArbitrator) Start() {
	m.t.Logf(
		"MockArbitrator.Start()",
	)
}

func (m *MockArbitrator) Stop() {
	if m.nextIndex != len(m.results) {
		panic("Invalid test case.")
	}

	m.t.Logf(
		"MockArbitrator.Stop()",
	)
}

func (m *MockArbitrator) Do([][][]*types.TxElement, *actor.WorkerThreadLogger) []*ethCommon.Hash {
	if m.nextIndex >= len(m.results) {
		panic(fmt.Sprintf("Invalid test case, nextIndex = %v, len(results) = %v", m.nextIndex, len(m.results)))
	}

	m.nextIndex++
	return m.results[m.nextIndex-1]
}
