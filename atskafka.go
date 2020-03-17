package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	socketPathFlag    = flag.String("socket", "/var/run/log.socket", "socket to communicate with fifo-log-demux")
	numericFieldsFlag = flag.String("numericFields", "time_firstbyte,response_size", "List of fields to be considered numeric")
	kafkaConfigFile   = flag.String("kafkaConfig", "/etc/atskafka.conf", "Kafka configuration file")
	kafkaTopicFlag    = flag.String("kafkaTopic", "test_topic", "Kafka topic")
	kafkaStatsFile    = flag.String("kafkaStatsFile", "/tmp/atskafka.stats.json", "File for Kafka JSON statistics")
)

func reader(c chan string) error {
	// Connect to fifo-log-demux socket
	conn, err := net.Dial("unix", *socketPathFlag)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send empty regexp to fifo-log-demux
	_, err = conn.Write([]byte(" "))
	if err != nil {
		return err
	}

	// Read lines from socket
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		c <- scanner.Text()
	}
	return nil
}

func doWork(c chan string, i int, p *kafka.Producer) {
	log.Println("Worker", i, "started")

	// List of fields not to be considered strings (eg: response_size)
	numericFields := make(map[string]bool)
	for _, field := range strings.Split(*numericFieldsFlag, ",") {
		numericFields[field] = true
	}

	for {
		line := <-c

		json, err := logLineToJson(line, numericFields)
		if err == nil {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: kafkaTopicFlag, Partition: kafka.PartitionAny},
				Value:          json,
			}, nil)
		} else {
			log.Printf("Error converting %v to JSON: %v\n", line, err)
		}
	}
}

// Delivery report handler for produced messages
func deliveryReport(p *kafka.Producer, stats *os.File) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				// TODO: proper handling of delivery error
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
			}
		case *kafka.Stats:
			if _, err := stats.Write([]byte(e.String())); err != nil {
				log.Printf("Error writing stats: %v\n", err)
			}
		}
	}
}

func main() {
	flag.Parse()

	c := make(chan string)

	p, err := kafka.NewProducer(loadConfig(*kafkaConfigFile))

	if err != nil {
		log.Fatal("Fatal error: ", err)
	}
	defer p.Close()

	stats, err := os.OpenFile(*kafkaStatsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer stats.Close()

	go deliveryReport(p, stats)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt)
	go func() {
		for sig := range sc {
			log.Printf("captured %v, exiting..", sig)
			os.Exit(0)
		}
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		go doWork(c, i, p)
	}

	for {
		// By keeping this in an infinite loop fifo-log-tailer is able to re-open
		// the UNIX socket after an error
		err := reader(c)
		if err != nil {
			log.Println("Unable to read from socket: {}", err)
			break
		}
	}
}
