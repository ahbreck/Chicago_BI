package shared

import (
	"log"
	"net"
	"net/http"
	"time"
)

// Declare transports and clients once for better performance and stability

// Shared simple client (for fast APIs)
var simpleTransport = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 300 * time.Second,
}

var simpleClient = &http.Client{
	Transport: simpleTransport,
	Timeout:   10 * time.Second,
}

// Shared extended-timeout client (for slow APIs, i.e., trips datasets)
var slowTransport = &http.Transport{
	MaxIdleConns:          10,
	IdleConnTimeout:       1000 * time.Second,
	TLSHandshakeTimeout:   1000 * time.Second,
	ExpectContinueTimeout: 1000 * time.Second,
	DisableCompression:    true,
	Dial: (&net.Dialer{
		Timeout:   1000 * time.Second,
		KeepAlive: 1000 * time.Second,
	}).Dial,
	ResponseHeaderTimeout: 1000 * time.Second,
}

var slowClient = &http.Client{
	Transport: slowTransport,
	Timeout:   1200 * time.Second,
}

// API fetch functions
func FetchFastAPI(url string) (*http.Response, error) {
	res, err := simpleClient.Get(url)
	if err != nil {
		log.Printf("Error fetching %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		log.Printf("Unexpected status: %d", res.StatusCode)
	}
	return res, nil
}

func FetchSlowAPI(url string) (*http.Response, error) {
	res, err := slowClient.Get(url)
	if err != nil {
		log.Printf("Error fetching %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		log.Printf("Unexpected status: %d", res.StatusCode)
	}
	return res, nil
}
