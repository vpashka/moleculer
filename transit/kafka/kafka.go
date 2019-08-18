package kafka

// This is a alpha version. Consumer Group is not supported.

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

const defaultKafkaVersion = "0.10.2.0"
const kafkaTopicBadSymbols = "[+-]"

type KafkaOptions struct {
	conf        *sarama.Config
	GroupID     string
	Brokers     []string
	Logger      *log.Entry
	Serializer  serializer.Serializer
	ValidateMsg transit.ValidateMsgFunc
}

type KafkaSubscriber struct {
	handler    transit.TransportHandler
	serializer serializer.Serializer
	cancel     chan struct{}
	topic      string
	logger     *log.Entry
}

type KafkaTransporter struct {
	prefix        string
	opts          *KafkaOptions
	client        sarama.Consumer
	producer      sarama.AsyncProducer
	logger        *log.Entry
	serializer    serializer.Serializer
	subscriptions map[string]*KafkaSubscriber
	subMut        sync.Mutex
	wg            sync.WaitGroup
	cancel        chan struct{}
	badSymbols    *regexp.Regexp
}

func GetDefaultOptions() *KafkaOptions {

	version, err := sarama.ParseKafkaVersion(defaultKafkaVersion)
	if err != nil {
		msg := fmt.Sprint("kafka options: Error parsing Kafka version: ", defaultKafkaVersion, " err: ", err)
		panic(errors.New(msg))
	}

	conf := sarama.NewConfig()
	conf.Version = version
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest
	return &KafkaOptions{
		conf:    conf,
		GroupID: "",
	}
}

func kafkaOptions(options KafkaOptions) *KafkaOptions {
	opts := GetDefaultOptions()
	opts.GroupID = options.GroupID
	opts.Brokers = options.Brokers
	return opts
}

func CreateKafkaTransporter(options KafkaOptions) transit.Transport {

	opts := kafkaOptions(options)

	r, err := regexp.Compile(kafkaTopicBadSymbols)
	if err != nil {
		options.Logger.Fatalf("kafka transporter error: %s", err)
	}

	return &KafkaTransporter{
		opts:          opts,
		logger:        options.Logger,
		serializer:    options.Serializer,
		subscriptions: make(map[string]*KafkaSubscriber),
		cancel:        make(chan struct{}),
		badSymbols:    r,
	}
}

func (t *KafkaTransporter) createProducer() (sarama.AsyncProducer, error) {

	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewRandomPartitioner
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = false
	// kconf.ChannelBufferSize = conf.producerQueueSize
	conf.ClientID = t.opts.GroupID

	producer, err := sarama.NewAsyncProducer(t.opts.Brokers, conf)
	if err != nil {
		t.logger.Errorf("Failed to start kafka producer: %s", err)
		return nil, errors.New(fmt.Sprintf("Failed to start kafka producer: %s", err))
	}

	return producer, nil
}

func (t *KafkaTransporter) Connect() chan error {
	endChan := make(chan error)

	go func() {

		if t.client != nil && t.producer != nil {
			endChan <- nil
			return
		}

		t.logger.Debug("Kafka Connect() - brokers: ", t.opts.Brokers, " GroupID: ", t.opts.GroupID)

		// client, err := sarama.NewConsumerGroup(t.opts.Brokers, t.opts.GroupID, t.opts.conf)
		client, err := sarama.NewConsumer(t.opts.Brokers, t.opts.conf)
		if err != nil {
			t.logger.Error("Kafka Connect() - Create consumer error: ", err, "brokers: ", t.opts.Brokers, " GroupID: ", t.opts.GroupID)
			endChan <- errors.New(fmt.Sprint("kafka: create consumer error: ", err, " brokers: ", t.opts.Brokers))
			return
		}

		producer, err := t.createProducer()
		if err != nil {
			client.Close()
			t.logger.Error("Kafka Connect() - Create producer error: ", err)
			endChan <- errors.New(fmt.Sprint("kafka: create producer error: ", err))
			return
		}

		t.logger.Info("Connected to ", t.opts.Brokers)
		t.client = client
		t.producer = producer
		endChan <- nil
	}()
	return endChan
}

func (t *KafkaTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		if t.client == nil && t.producer == nil {
			endChan <- nil
			return
		}

		t.logger.Info("disconnect client..")

		t.subMut.Lock()
		for _, sub := range t.subscriptions {
			sub.cancel <- struct{}{}
		}
		// reset subscribers
		t.subscriptions = make(map[string]*KafkaSubscriber)
		t.subMut.Unlock()

		t.wg.Wait()

		if t.client != nil {
			err := t.client.Close()
			if err != nil {
				t.logger.Errorf("Close client error: %s", err)
			}
			t.client = nil
		}

		if t.producer != nil {
			err := t.producer.Close()
			if err != nil {
				t.logger.Errorf("Close producer error: %s", err)
			}
			t.producer = nil
		}
		endChan <- nil
	}()
	return endChan
}

func (t *KafkaTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}

func (t *KafkaTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {

	if t.client == nil {
		msg := fmt.Sprint("kafka.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)

	t.logger.Debug("kafka.Subscribe(): command: ", command, " nodeID: ", nodeID)

	if t.badSymbols.MatchString(topic) {
		msg := fmt.Sprintf("kafka.Subscribe() error: Bad symbols in topic name '%s'. Don't use %s", topic, kafkaTopicBadSymbols)
		t.logger.Warn(msg)
		panic(errors.New(msg)) // panic?!
	}

	t.subMut.Lock()
	defer t.subMut.Unlock()

	sub, ok := t.subscriptions[topic]
	if ok {
		t.logger.Debug("kafka.Subscribe(): Already subsribed: topic: ", topic)
		return
	}

	sub = &KafkaSubscriber{
		handler:    handler,
		cancel:     make(chan struct{}, 1),
		topic:      topic,
		logger:     t.logger.WithField("topic", topic),
		serializer: t.serializer,
	}

	t.subscriptions[topic] = sub
	go sub.mainLoop(t.client)
}

func (t *KafkaTransporter) Publish(command, nodeID string, message moleculer.Payload) {

	if t.producer == nil {
		msg := fmt.Sprint("kafka.Publish() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)
	t.logger.Debug("kafka.Publish() command: ", command, " topic: ", topic, " nodeID: ", nodeID)
	t.logger.Trace("message: \n", message, "\n - end")

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(t.serializer.PayloadToBytes(message)),
	}

	select {
	case t.producer.Input() <- msg:
	case err := <-t.producer.Errors():
		t.logger.Error("Error on publish: error: ", err, " command: ", command, " topic: ", topic)
		panic(err) // panic ?
	}
}

func (t *KafkaTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

// ----------------------------------------------------------------------------
func (k *KafkaSubscriber) mainLoop(client sarama.Consumer) {

	k.logger.Debug("kafka: start consumer for topic: ", k.topic)

	partitions, err := client.Partitions(k.topic)
	if err != nil {
		k.logger.Panicf("kafka: get partitions for topic '%s' error: %s", k.topic, err)
	}

	type ConsumerInfo struct {
		consumer sarama.PartitionConsumer
		cancel   chan struct{}
	}

	// create partition consumers
	consumers := make([]ConsumerInfo, 0, len(partitions))
	for _, p := range partitions {

		// OffsetNewest ? - move to config
		c, err := client.ConsumePartition(k.topic, p, sarama.OffsetNewest)
		if err != nil {
			k.logger.Panicf("kafka: create partition consumer topic %s  error: %s", k.topic, err)
		}

		ci := ConsumerInfo{
			consumer: c,
			cancel:   make(chan struct{}, 1),
		}

		consumers = append(consumers, ci)
	}

	var wg sync.WaitGroup

	msgChan := make(chan *sarama.ConsumerMessage)

	// consume messages by partitions
	for _, c := range consumers {
		wg.Add(1)

		go func(cons ConsumerInfo) {

			defer wg.Done()
			defer cons.consumer.Close()

			for {
				select {
				case msg, ok := <-cons.consumer.Messages():
					if !ok {
						return
					}
					msgChan <- msg

				case cerr, ok := <-cons.consumer.Errors():
					if !ok {
						return
					}
					k.logger.Errorf("consume partition error: %s", cerr.Error())

				case <-cons.cancel:
					return
				}
			}
		}(c)
	}

MainLoop:
	for {
		select {
		case <-k.cancel:
			break MainLoop
		case msg := <-msgChan:
			// Message processing
			k.logger.Debug(fmt.Sprintf("Incoming kafka message topic=%s offset=%d partition=%d", msg.Topic, msg.Offset, msg.Partition))
			payload := k.serializer.BytesToPayload(&msg.Value)
			k.logger.Debug(fmt.Sprintf("Incoming %s packet from '%s'", msg.Topic, payload.Get("sender").String()))
			k.handler(payload)
		}
	}

	k.logger.Debug("Terminate consumer for topic: ", k.topic)

	for _, c := range consumers {
		c.cancel <- struct{}{}
	}

	wg.Wait()
}
