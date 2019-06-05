package handler


type Code int32

const (
	InternalErrCode Code = 501
	SearchAfterErrCode Code = 10001
)

func (c Code) String() string {
	var ret string
	switch c {
	case 501:
		ret = "ping failed"
	case 10001:
		ret = "search after failed"
	default:
		ret = "unknown error"
	}
	return ret
}
