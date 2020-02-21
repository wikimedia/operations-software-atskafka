all: clean atskafka

clean:
	-rm atskafka

atskafka:
	go fmt
	go build
