package collector

import (
	"net/http"
	"time"
)

var httpClient *http.Client

const (
	TIMEOUT = 30 * time.Second
	esURL   = "http://elasticsearch:9200"
)

func init() {
	httpClient = &http.Client{
		Timeout:   TIMEOUT,
		Transport: &http.Transport{},
	}
}
