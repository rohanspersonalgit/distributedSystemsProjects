package main

import (
	"flag"
	"fmt"
	"log"

	distkvs "example.org/cpsc416/a6"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("config/storage_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	var storagePort string

	flag.StringVar(&config.StorageID, "id", config.StorageID, "Storage ID, e.g. storage1")
	flag.StringVar(&storagePort, "port", string(config.StorageAdd), "Listening port")
	flag.StringVar(&config.DiskPath, "path", string(config.StorageAdd), "disk path")
	flag.Parse()

	fmt.Println(config)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.StorageID,
		Secret:         config.TracerSecret,
	})
	log.Println(config.StorageID)
	storage := distkvs.Storage{}

	err = storage.Start(config.StorageID, config.FrontEndAddr, storagePort, config.DiskPath, tracer)
	if err != nil {
		log.Fatal(err)
	}
}
