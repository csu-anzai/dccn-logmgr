package collector

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/golang/glog"
)

var (
	ErrFailedClusterInfo = errors.New("failed to get cluster info")
)

func GetClusterInfo(url string) (*ClusterInfoResponse, error) {
	var clusterInfo ClusterInfoResponse

	client := http.Client{}
	res, err := client.Get(url)
	if err != nil {
		glog.Errorf("failed to get cluster info, %v", err)
		return nil, ErrFailedClusterInfo
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			glog.Errorf("failed to close http client, %v", err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return nil, ErrHttpStatus
	}

	if err := json.NewDecoder(res.Body).Decode(&clusterInfo); err != nil {
		glog.Errorf("failed to decode cluster info, %v", err)
		return nil, err
	}
	return &clusterInfo, nil
}
