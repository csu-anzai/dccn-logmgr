package handler

import (
	ankr_default "github.com/Ankr-network/dccn-common/protos"
)

type Code int32

const (
	SuccessCode        Code = 200
	InternalErrCode    Code = 10000
	SearchAfterErrCode Code = 10001
	CountErrCode       Code = 10002
)

func (c Code) String() string {
	var ret string
	switch c {
	case 200:
		ret = "SUCCESS"
	case 10000:
		ret = ankr_default.ErrElasticsearchPing.Error()
	case 10001:
		ret = ankr_default.ErrElasticsearchSearchAfter.Error()
	case 10002:
		ret = ankr_default.ErrElasticsearchCount.Error()
	default:
		ret = "unknown error"
	}
	return ret
}
