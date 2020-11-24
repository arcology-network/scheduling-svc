package workers

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	schedulingTypes "github.com/HPISTechnologies/scheduling-svc/service/types"
)

type MockExecutor struct {
	t         *testing.T
	responses [][]*types.ExecuteResponse
	nextIndex int
}

func NewMockExecutor(t *testing.T, responses [][]*types.ExecuteResponse) *MockExecutor {
	return &MockExecutor{
		t:         t,
		responses: responses,
	}
}

func (m *MockExecutor) Start() {
	m.t.Logf(
		"MockExecutor.Start()",
	)
}

func (m *MockExecutor) Stop() {
	if m.nextIndex != len(m.responses) {
		panic("Invalid test case.")
	}

	m.t.Logf(
		"MockExecutor.Stop()",
	)
}

func (m *MockExecutor) Run(msgs []*schedulingTypes.Message, timestamp *big.Int, msgTemplate *actor.Message) []*types.ExecuteResponse {
	if m.nextIndex >= len(m.responses) {
		panic(fmt.Sprintf("Invalid test case, nextIndex = %v, len(responses) = %v", m.nextIndex, len(m.responses)))
	}

	m.nextIndex++
	return m.responses[m.nextIndex-1]
}
