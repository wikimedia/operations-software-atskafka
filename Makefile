all: clean atskafka

clean:
	-rm atskafka

atskafka:
	go fmt
	GOPATH=/usr/share/gocode go build

test:
	GOCACHE=/tmp GOPATH=/usr/share/gocode go test -bench . -v
