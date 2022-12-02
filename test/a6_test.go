package test

import (
	"context"
	distkvs "example.org/cpsc416/a6"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSanityA6(t *testing.T){
	setup := setupServer(t)
	clistart, clicancel := context.WithTimeout(context.Background(), 5*time.Minute)
	startBinary(setup.path,"/client", clistart)
	defer clicancel()

	defer setup.cancelAll()
}


func TestRunClientTwiceA6(t *testing.T){
	setup := setupServer(t)
	defer setup.cancelAll()
	//var config distkvs.ClientConfig
	//
	//err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	//flag.Parse()
	//
	//client := distkvs.NewClient(config, kvslib.NewKVS())
	//if err := client.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	//
	//if err, _ := client.Put("client1", "key1", "value2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client.Get("client1", "key1"); err != 0 {
	//	log.Println(err)
	//}
	//for i := 0; i < 2; i++ {
	//	result := <-client.NotifyChannel
	//	log.Println(result)
	//}
	//
	//client.Close()
	//
	//client = distkvs.NewClient(config, kvslib.NewKVS())
	//if err = client.Initialize(); err != nil {
	//	log.Fatal(err)
	//}
	//
	//if err, _ := client.Put("client1", "key1", "value2"); err != 0 {
	//	log.Println(err)
	//}
	//if err, _ := client.Get("client1", "key1"); err != 0 {
	//	log.Println(err)
	//}
	//for i := 0; i < 2; i++ {
	//	result := <-client.NotifyChannel
	//	log.Println(result)
	//}
	//client.Close()
	run2Clients()
	runGrader("./gradera6-smoketest -s storage0 storage1 storage2 storage3 --trace-file ../trace_output.log --client-id client1 client2 --req client2:put:1:key1 client2:put:1:key2 client1:put:1:key1 client1:put:1:key2 client2:get:2:key1 client2:get:2:key2 client1:get:2:key2 ")
}
func setupServer(t *testing.T) *SetupState{

	fmt.Println("Begin setup")
	setup := clearPorts(t)
	go startBinary(setup.path, "/tracing-server", setup.tracingServerContext)
	time.Sleep(2 * time.Second)

	setup.frontendContext, setup.frontendCancel = context.WithTimeout(context.Background(), 5*time.Minute)
	go startBinary(setup.path, "/frontend", setup.frontendContext)
	time.Sleep(2 * time.Second)

	var storageConfig distkvs.StorageConfig
	err := distkvs.ReadJSONConfig("../config/storage_config.json", &storageConfig)
	if err != nil {
		t.Fatal("error reading config")
	}
	setup.storageContext = make([]context.Context, setup.numStorages)
	setup.storageCancel = make([]func(), setup.numStorages)
	for i:= 0; i<setup.numStorages; i++ {
		id:= "storage" + strconv.Itoa(i)
		port, err := strconv.Atoi(string(storageConfig.StorageAdd)[1:])
		if err != nil {
			return nil
		}
		port += i
		stringPort := ":"
		stringPort += strconv.Itoa(port)
		setup.storageContext[i], setup.storageCancel[i] = context.WithTimeout(context.Background(), 5*time.Minute)
		go startBinary(setup.path, "/storage",setup.storageContext[i], "-id", id, "-port", stringPort)
	}
	time.Sleep(5*time.Second)
	fmt.Print("end setup")
	return setup
}

func clearPorts(t *testing.T) *SetupState {
	stdout := executeCommand("pwd")

	setup := new(SetupState)
	//set num storages here
	setup.numStorages = 4

	path := string(stdout)
	setup.path = path[:strings.LastIndex(path, "/")]
	log.Println("Working directory: path")
	var tracingServerConfig TracingServerConfig
	err := distkvs.ReadJSONConfig("../config/tracing_server_config.json", &tracingServerConfig)
	if err != nil {
		t.Fatal("error reading config")
	}
	killProcessOnPort(tracingServerConfig.ServerBind)
	setup.tracingServerContext, setup.tracingServerCancel = context.WithTimeout(context.Background(), 5*time.Minute)
	var frontendConfig distkvs.FrontEndConfig
	err = distkvs.ReadJSONConfig("../config/frontend_config.json", &frontendConfig)
	if err != nil {
		t.Fatal("error reading config")
	}

	killProcessOnPort(frontendConfig.ClientAPIListenAddr)
	killProcessOnPort(frontendConfig.StorageAPIListenAddr)

	var storageConfig distkvs.StorageConfig
	err = distkvs.ReadJSONConfig("../config/storage_config.json", &storageConfig)
	if err != nil {
		t.Fatal("error reading config")
	}
	_, err = os.Stat(storageConfig.DiskPath)
	if err == nil {
		if os.RemoveAll("/tmp/newdir/") != nil {
			log.Fatal("error emptying storafe")
		}
		if os.Mkdir("/tmp/newdir", os.ModePerm) != nil {
			log.Fatal("creating new dir for storage")
		}
		for i:= 0; i<4; i++{
			fileName:= "storage" + strconv.Itoa(i)
			if os.Mkdir("/tmp/newdir/" + fileName, os.ModePerm) != nil {
				log.Fatal("creating new dir for storage error")
			}
		}
	}

	startPort, err := strconv.Atoi(string(storageConfig.StorageAdd)[1:])
	if err != nil {
		return nil
	}
	for i := 0; i<setup.numStorages ; i++ {
		killPort := startPort + i
		port  := strconv.Itoa(killPort)
		killProcessOnPort(port)
	}
	return setup
}