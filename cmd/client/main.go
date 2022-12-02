package main

import (
	distkvs "example.org/cpsc416/a6"
	"example.org/cpsc416/a6/kvslib"
	"flag"
	"fmt"
	"log"
	"math/rand"
)

func putNKeys(clientId string, client *distkvs.Client, n int) {
	go func() {
		for i := 0; i < n; i++ {
			if err, _ := client.Put(clientId, "key"+fmt.Sprint(i), "value"+fmt.Sprint(rand.Intn(100))); err != 0 {
				log.Println(err)
			}
		}
	}()
}

func getNKeys(clientId string, client *distkvs.Client, n int) {
	go func() {
		for i := 0; i < n; i++ {
			if err, _ := client.Get(clientId, "key"+fmt.Sprint(i)); err != 0 {
				log.Println(err)
			}
		}
	}()
}

func clientStart(clientId string, client *distkvs.Client, N int) {
	putNKeys(clientId, client, N)
	putNKeys(clientId, client, N)
	putNKeys(clientId, client, N)

	getNKeys(clientId, client, N)
	getNKeys(clientId, client, N)
	getNKeys(clientId, client, N)
}

func clientEnd(client *distkvs.Client, N int) {
	for i := 0; i < N*3*2; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
}

func main() {
	var config distkvs.ClientConfig
	err := distkvs.ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client1 := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client1.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client1.Close()

	config.ClientID = "client2"
	client2 := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client2.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client2.Close()

	config.ClientID = "client3"
	client3 := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client3.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client3.Close()

	config.ClientID = "client4"
	client4 := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client4.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client4.Close()

	N := 4

	clientStart("client1", client1, N)
	clientStart("client2", client2, N)
	clientStart("client3", client3, N)
	clientStart("client4", client4, N)

	clientEnd(client1, N)
	clientEnd(client2, N)
	clientEnd(client3, N)
	clientEnd(client4, N)
	testString := createString("client1", N, "put")
	testString += createString("client2", N, "put")
	testString += createString("client3", N, "put")
	testString += createString("client4", N, "put")
	testString += createString("client1", N, "get")
	testString += createString("client2", N, "get")
	testString += createString("client3", N, "get")
	testString += createString("client4", N, "get")
	defer log.Println(testString)
}

func createString(clientid string, N int, command string) string {
	testString := ""
	for i := 0; i < N; i++ {
		testString += clientid + ":" + command + ":" + fmt.Sprint(3) + ":key" + fmt.Sprint(i) + " "
	}
	return testString
}

//client1:put:3:key0 client1:put:3:key1 client1:put:3:key2 client1:put:3:key3 client1:put:3:key4 client1:put:3:key5 client1:put:3:key6 client1:put:3:key7 client1:put:3:key8 client1:put:3:key9 client2:put:3:key0 client2:put:3:key1 client2:put:3:key2 client2:put:3:key3 client2:put:3:key4 client2:put:3:key5 client2:put:3:key6 client2:put:3:key7 client2:put:3:key8 client2:put:3:key9 client3:put:3:key0 client3:put:3:key1 client3:put:3:key2 client3:put:3:key3 client3:put:3:key4 client3:put:3:key5 client3:put:3:key6 client3:put:3:key7 client3:put:3:key8 client3:put:3:key9 client4:put:3:key0 client4:put:3:key1 client4:put:3:key2 client4:put:3:key3 client4:put:3:key4 client4:put:3:key5 client4:put:3:key6 client4:put:3:key7 client4:put:3:key8 client4:put:3:key9 client1:get:3:key0 client1:get:3:key1 client1:get:3:key2 client1:get:3:key3 client1:get:3:key4 client1:get:3:key5 client1:get:3:key6 client1:get:3:key7 client1:get:3:key8 client1:get:3:key9 client2:get:3:key0 client2:get:3:key1 client2:get:3:key2 client2:get:3:key3 client2:get:3:key4 client2:get:3:key5 client2:get:3:key6 client2:get:3:key7 client2:get:3:key8 client2:get:3:key9 client3:get:3:key0 client3:get:3:key1 client3:get:3:key2 client3:get:3:key3 client3:get:3:key4 client3:get:3:key5 client3:get:3:key6 client3:get:3:key7 client3:get:3:key8 client3:get:3:key9 client4:get:3:key0 client4:get:3:key1 client4:get:3:key2 client4:get:3:key3 client4:get:3:key4 client4:get:3:key5 client4:get:3:key6 client4:get:3:key7 client4:get:3:key8 client4:get:3:key9
