package main

import "testing"

func TestLogLineToJson(t *testing.T) {
	const l = "hostname:cp3050.esams.wmnet	time_firstbyte:1235	http_method:GET	uri_host:en.wikipedia.org"
	const j = `{"hostname": "cp3050.esams.wmnet", "time_firstbyte": 1235, "http_method": "GET", "uri_host": "en.wikipedia.org"}`

	m := map[string]bool{"time_firstbyte": true}
	r := logLineToJson(l, m)

	if r != j {
		t.Errorf("Got j=%s", j)
	}
}

func TestValidJSON(t *testing.T) {
	const s = `{"hostname": "cp3050.esams.wmnet",
                "time_firstbyte": 167,
                "ip": "127.0.0.1",
                "cache_status": "pass",
                "http_status": "200",
                "response_size": 1235,
                "http_method": "GET",
                "uri_host": "en.wikipedia.org",
                "uri_path": "/w/api.php?format=json&action=query&titles=File:No.JPG",
                "content_type": "application/json; charset=utf-8",
                "referer": "-",
                "user_agent": "-",
                "accept_language": "-",
                "x_analytics": "ns=-1;special=Badtitle;https=1;nocookies=1",
                "range": "-",
                "x_cache": "cp3052 miss, cp3050 pass",
                "accept": "-",
                "backend": "mw1347.eqiad.wmnet",
                "tls": "vers=TLSv1.2;keyx=X25519;auth=ECDSA;ciph=CHACHA20-POLY1305-SHA256;prot=h1;sess=reused"}`

	if !validJSON(s, nil) {
		t.Errorf("Input expected to be valid JSON")
	}
}

func TestInvalidJSON(t *testing.T) {
	const s = `{"hostname": "cp3050.esams.wmnet"`

	if validJSON(s, nil) {
		t.Errorf("Input expected to be invalid JSON")
	}
}
