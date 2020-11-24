package types

import (
	"github.com/HPISTechnologies/common-lib/types"
)

type ReaperCommand struct {
	NodeRole    *types.NodeRole
	MaxReap     int
	Reapinglist *types.ReapingList
}
