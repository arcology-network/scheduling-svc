package types

type CheckList struct {
	blackList  map[string]interface{}
	dataRange  int
	removeList [][]string
}

func NewCheckList(savedRange int) *CheckList {
	return &CheckList{
		blackList:  map[string]interface{}{},
		dataRange:  savedRange,
		removeList: [][]string{},
	}
}

func (cl *CheckList) OnReceivedList(list []string) {
	cl.removeList = append(cl.removeList, list)

	for _, key := range list {
		cl.blackList[key] = 0
	}

	if len(cl.removeList) > cl.dataRange {
		for _, key := range cl.removeList[0] {
			delete(cl.blackList, key)
		}
		cl.removeList = cl.removeList[1:]
	}
}

func (cl *CheckList) Exist(key string) bool {
	if _, ok := cl.blackList[key]; ok {
		return true
	} else {
		return false
	}
}
