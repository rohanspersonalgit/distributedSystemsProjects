package distkvs

import (
	"errors"
	"log"

	"example.org/cpsc416/a6/kvslib"
	"github.com/DistributedClocks/tracing"
)

const ChCapacity = 10

type ClientConfig struct {
	ClientID         string
	FrontEndAddr     string
	TracerServerAddr string
	TracerSecret     []byte
}

type Client struct {
	NotifyChannel kvslib.NotifyChannel
	id            string
	frontEndAddr  string
	kvs           *kvslib.KVS
	tracer        *tracing.Tracer

	initialized   bool
	tracerConfig  tracing.TracerConfig
}

func NewClient(config ClientConfig, kvs *kvslib.KVS) *Client {
	log.Println(config.ClientID + "here")
	tracerConfig := tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.ClientID,
		Secret:         config.TracerSecret,
	}
	return &Client{
		NotifyChannel: nil,
		id:            config.ClientID,
		frontEndAddr:  config.FrontEndAddr,
		kvs:           kvs,
		tracer:        &tracing.Tracer{},
		initialized:   false,
		tracerConfig:  tracerConfig,
	}
}

func (c *Client) Initialize() error {
	if c.initialized {
		return errors.New("client has been initialized before")
	}
	c.tracer = tracing.NewTracer(c.tracerConfig)
	ch, err := c.kvs.Initialize(c.tracer, c.id, c.frontEndAddr, ChCapacity)
	c.NotifyChannel = ch
	c.initialized = true
	return err
}

func (c *Client) Get(clientId string, key string) (uint32, error) {
	return c.kvs.Get(c.tracer, clientId, key)
}

func (c *Client) Put(clientId string, key string, value string) (uint32, error) {
	return c.kvs.Put(c.tracer, clientId, key, value)
}

func (c *Client) Close() error {
	if err := c.kvs.Close(); err != nil {
		return err
	}
	if err := c.tracer.Close(); err != nil {
		return err
	}
	c.initialized = false
	return nil
}
