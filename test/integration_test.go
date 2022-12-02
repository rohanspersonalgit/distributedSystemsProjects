package test

import (
	"context"
	distkvs "example.org/cpsc416/a6"
	"example.org/cpsc416/a6/kvslib"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

type TracingServerConfig struct {
	ServerBind       string
	Secret           string
	OutputFile       string
	ShivizOutputFile string
}

type SetupState struct {
	path                 string
	tracingServerCancel  func()
	tracingServerContext context.Context

	frontendCancel  func()
	frontendContext context.Context

	storageCancel  []func()
	storageContext []context.Context
	numStorages int
}

func TestSanityWithSetup(t *testing.T) {
	setup := setupServers(t)
	defer setup.cancelAll()
	runClient()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 --req client1:put:1:key1 client1:get:1:key1")
}

func TestSanity(t *testing.T) {
	runClient()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 --req client1:put:1:key1 client1:get:1:key1")
}

func TestRunGrader(t *testing.T) {
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 --req client1:put:1:key1 client1:get:1:key1")
}

func Test2ClientsWithSetup(t *testing.T) {
	setup := setupServers(t)
	defer setup.cancelAll()
	run2Clients()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 client2 --req client2:put:1:key1 client2:put:1:key2 client1:put:1:key1 client1:put:1:key2 client2:get:2:key1 client2:get:2:key2 client1:get:2:key2")
}

func TestStress3Clients10Put10Get(t *testing.T) {
	run3ClientsStress10Put10Get()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 client2 client3 --req client1:put:5:key1 client1:put:5:key2 client2:put:5:key1 client2:put:5:key2 client3:put:5:key1 client3:put:5:key2 client1:get:5:key1 client1:get:5:key2 client2:get:5:key1 client2:get:5:key2 client3:get:5:key1 client3:get:5:key2")
}

func TestStress1Clien4Put4Get(t *testing.T) {
	run1ClientStress4Put4Get()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 --req client1:put:2:key1 client1:put:2:key2 client1:get:2:key1 client1:get:2:key2")
}

func TestStress1Client20Put20Get(t *testing.T) {
	run1ClientStress4Put4Get()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 --req client1:put:10:key1 client1:put:10:key2 client1:get:10:key1 client1:get:10:key2")
}

func TestStress3ClientsMANY(t *testing.T) {
	run3ClientsStressMANY()
	runGrader("./grader --trace-file ../trace_output.log --client-id client1 client2 client3 --req client1:put:50:key1 client1:put:50:key2 client2:put:50:key1 client2:put:50:key2 client3:put:50:key1 client3:put:50:key2 client1:get:50:key1 client1:get:50:key2 client2:get:50:key1 client2:get:50:key2 client3:get:50:key1 client3:get:50:key2")
}

func TestRunClientTwice(t *testing.T) {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}

	if err, _ := client.Put("client1", "key1", "value2"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}
	for i := 0; i < 2; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}

	client.Close()

	client = distkvs.NewClient(config, kvslib.NewKVS())
	if err = client.Initialize(); err != nil {
		log.Fatal(err)
	}

	if err, _ := client.Put("client1", "key1", "value2"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}
	for i := 0; i < 2; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}

	client.Close()

	runGrader("./grader --trace-file ../trace_output.log --client-id client1 client2 client3 --req client1:put:5:key1 client1:put:5:key2 client2:put:5:key1 client2:put:5:key2 client3:put:5:key1 client3:put:5:key2 client1:get:5:key1 client1:get:5:key2 client2:get:5:key1 client2:get:5:key2 client3:get:5:key1 client3:get:5:key2")
}

func TestClearPortsAndStorage(t *testing.T) {
	clearPortsAndStorage(t)
}

func runGrader(runScript string) {
	cmd := exec.Command("bash", "-c", runScript)
	stdout, err := cmd.Output()
	if err != nil {
		// if error -> most liekly port is closed
		return
	}
	log.Println(string(stdout))

	_, err = os.Stat("grader.log")
	if err == nil {
		if os.Remove("grader.log") != nil {
			log.Fatal("error deleting grader log")
		}
	}
	err = ioutil.WriteFile("grader.log", stdout, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
}

func setupServers(t *testing.T) *SetupState {
	fmt.Println("Begin setup")
	setup := clearPortsAndStorage(t)

	go startBinary(setup.path, "/tracing-server", setup.tracingServerContext)

	// allow tracing to start
	time.Sleep(2 * time.Second)

	setup.frontendContext, setup.frontendCancel = context.WithTimeout(context.Background(), 5*time.Minute)
	go startBinary(setup.path, "/frontend", setup.frontendContext)

	// allow frontend to start
	time.Sleep(2 * time.Second)

	//setup.storageContext, setup.storageCancel = context.WithTimeout(context.Background(), 5*time.Minute)
	//go startBinary(setup.path, "/storage", setup.storageContext)

	// allow storage to start
	time.Sleep(2 * time.Second)

	fmt.Println("End setup")
	return setup
}

func clearPortsAndStorage(t *testing.T) *SetupState {
	stdout := executeCommand("pwd")

	setup := new(SetupState)

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

	// empty storage
	_, err = os.Stat(storageConfig.DiskPath)
	if err == nil {
		if os.RemoveAll("/tmp/newdir/") != nil {
			log.Fatal("error emptying storafe")
		}
		if os.Mkdir("/tmp/newdir", os.ModePerm) != nil {
			log.Fatal("creating new dir for storage")
		}
	}

	killProcessOnPort(string(storageConfig.StorageAdd))
	return setup
}

func runClient() {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&config.ClientID, "id", config.ClientID, "Client ID, e.g. client1")
	flag.Parse()

	client := distkvs.NewClient(config, kvslib.NewKVS())
	if err := client.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err, _ := client.Put("client1", "key1", "value2"); err != 0 {
		log.Println(err)
	}
	if err, _ := client.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}
	for i := 0; i < 2; i++ {
		result := <-client.NotifyChannel
		log.Println(result)
	}
}

func run2Clients() {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
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

	if err, _ := client2.Put("client2", "key1", "value1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Put("client1", "key1", "value2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "key2", "value3"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "key2", "value4"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Put("client2", "key1", "value5"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key1"); err != 0 {
		log.Println(err)
	}

	if err, _ := client1.Get("client1", "key2"); err != 0 {
		log.Println(err)
	}

	if err, _ := client2.Get("client2", "key2"); err != 0 {
		log.Println(err)
	}

	for i := 0; i < 5; i++ {
		result := <-client1.NotifyChannel
		log.Println(result)
	}
	for i := 0; i < 5; i++ {
		result := <-client2.NotifyChannel
		log.Println(result)
	}

}

func run3ClientsStress10Put10Get() {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
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

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client1.Put("client1", "key1", "value"+fmt.Sprint(i)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client1.Put("client1", "key2", "value"+fmt.Sprint(i*2)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client2.Put("client2", "key1", "value"+fmt.Sprint(i*3)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client2.Put("client2", "key2", "value"+fmt.Sprint(i*4)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client3.Put("client3", "key1", "value"+fmt.Sprint(i*5)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client3.Put("client3", "key2", "value"+fmt.Sprint(i*6)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client1.Get("client1", "key1"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client1.Get("client1", "key2"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client2.Get("client2", "key1"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client2.Get("client2", "key2"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client3.Get("client3", "key1"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			if err, _ := client3.Get("client3", "key2"); err != 0 {
				log.Println(err)
			}
		}
	}()

	for i := 0; i < 20; i++ {
		result := <-client1.NotifyChannel
		log.Println(result)
	}

	for i := 0; i < 20; i++ {
		result := <-client2.NotifyChannel
		log.Println(result)
	}

	for i := 0; i < 20; i++ {
		result := <-client3.NotifyChannel
		log.Println(result)
	}
}

func run3ClientsStressMANY() {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
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

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client1.Put("client1", "key1", "value"+fmt.Sprint(i)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client1.Put("client1", "key2", "value"+fmt.Sprint(i*2)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client2.Put("client2", "key1", "value"+fmt.Sprint(i*3)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client2.Put("client2", "key2", "value"+fmt.Sprint(i*4)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client3.Put("client3", "key1", "value"+fmt.Sprint(i*5)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client3.Put("client3", "key2", "value"+fmt.Sprint(i*6)); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client1.Get("client1", "key1"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client1.Get("client1", "key2"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client2.Get("client2", "key1"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client2.Get("client2", "key2"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client3.Get("client3", "key1"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			if err, _ := client3.Get("client3", "key2"); err != 0 {
				log.Println(err)
			}
			time.Sleep(time.Millisecond * 300)
		}
	}()

	for i := 0; i < 200; i++ {
		result := <-client1.NotifyChannel
		log.Println(result)
	}

	for i := 0; i < 200; i++ {
		result := <-client2.NotifyChannel
		log.Println(result)
	}

	for i := 0; i < 200; i++ {
		result := <-client3.NotifyChannel
		log.Println(result)
	}

	log.Println("DONE")
}

func run1ClientStress4Put4Get() {
	var config distkvs.ClientConfig

	err := distkvs.ReadJSONConfig("../config/client_config.json", &config)
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

	go func() {
		for i := 0; i < 10; i++ {
			if err, _ := client1.Put("client1", "key1", "value"+fmt.Sprint(i)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			if err, _ := client1.Put("client1", "key2", "value"+fmt.Sprint(i*2)); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			if err, _ := client1.Get("client1", "key1"); err != 0 {
				log.Println(err)
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			if err, _ := client1.Get("client1", "key2"); err != 0 {
				log.Println(err)
			}
		}
	}()

	for i := 0; i < 40; i++ {
		result := <-client1.NotifyChannel
		log.Println(result)
	}
}

func executeCommand(cmdStr string) []byte {
	cmd := exec.Command(cmdStr)
	stdout, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return stdout
}

func startBinary(path string, name string, ctx context.Context, args ...string) {
	tracingServerStr := path + name
	var tracingServerCmd *exec.Cmd
	if args != nil{
		tracingServerCmd = exec.CommandContext(ctx,tracingServerStr, "-id", args[1], "-port", args[3])
	} else{
		tracingServerCmd = exec.CommandContext(ctx, tracingServerStr)

	}
	tracingServerCmd.Dir = path

	stdout, err := tracingServerCmd.Output()
	if err != nil {
		log.Println("FAIL" + args[1] + args[3])
		log.Println(string(stdout))
		log.Fatal(err)
	}else{
		log.Println("PASS" + name)
	}
}

func killProcessOnPort(ipPortStr string) {
	ipPort := strings.Split(ipPortStr, ":")
	if len(ipPort) == 0 {
		log.Fatal("no port for config")
	}
	port := ipPort[len(ipPort)-1]
	log.Print(port)

	lsofArg := "lsof -i -P -n | grep " + port

	cmd := exec.Command("bash", "-c", lsofArg)
	stdout, err := cmd.Output()
	if err != nil {
		// if error -> most liekly port is closed
		return
	}
	log.Println(string(stdout))

	r, _ := regexp.Compile(".* (\\d+) roman.*")
	match := r.FindStringSubmatch(string(stdout))
	if err != nil {
		log.Fatal("Failed regex on " + string(stdout))
	}

	if len(match) == 2 {
		cmd := exec.Command("bash", "-c", "kill "+match[1])
		stdout, err = cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		log.Println(stdout)
	}
}

func (ss *SetupState) cancelAll() {
	fmt.Println("Begin teardown")
	ss.tracingServerCancel()
	ss.frontendCancel()
	for i := 0; i<ss.numStorages; i++ {
		ss.storageCancel[i]()
	}
	fmt.Println("End setup")
}
