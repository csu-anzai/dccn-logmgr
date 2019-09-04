package collector

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/golang/glog"
)

var (
	ErrFailedClusterInfo     = errors.New("failed to get cluster info")
	ErrFailedClusterInfoBody = errors.New("failed to read cluster info body")
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
	} else {
		//body, err := ioutil.ReadAll(res.Body)
		//if err != nil {
		//	glog.Errorf("failed to read cluster info body, %v", err)
		//	return nil, ErrFailedClusterInfoBody
		//}
		//glog.V(3).Infof("read cluster info body: %s", string(body))
	}

	if err := json.NewDecoder(res.Body).Decode(&clusterInfo); err != nil {
		glog.Errorf("failed to decode cluster info, %v", err)
		return nil, err
	}
	return &clusterInfo, nil
}
