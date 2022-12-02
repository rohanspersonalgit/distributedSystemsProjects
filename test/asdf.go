package distkvs

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"example.org/cpsc416/a6/kvslib"
)

type req struct {
	isGet    bool
	key      string
	value    string
	expected string
}

type clientReqs struct {
	clientID string
	reqs     []req
}

type testCase struct {
	name       string
	dirname    string
	numStorage int
	requests   []clientReqs
}

const testoutDir string = "test_outputs/"

func getTestCases() []testCase {
	testCases := []testCase{
		{
			name:       "A test with one test client running a simple test case",
			dirname:    "simple-grade/",
			numStorage: 5,
			requests: []clientReqs{
				{
					clientID: "client1",
					reqs: []req{
						{
							false,
							"1",
							"2",
							"pass",
						},
						{
							true,
							"1",
							"",
							"2",
						},
					},
				},
			},
		},
		{
			name:       "A test with one test client running a complicated case",
			dirname:    "complicated-grade/",
			numStorage: 2,
			requests: []clientReqs{
				{
					clientID: "client1",
					reqs: []req{
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value1",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value1",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value2",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value2",
						},
					},
				},
			},
		},
		{
			name:       "Complicated test with 2 clients",
			dirname:    "complicated-2clients/",
			numStorage: 3,
			requests: []clientReqs{
				{
					clientID: "client1",
					reqs: []req{
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value1",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value1",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value2",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value2",
						},
					},
				},
				{
					clientID: "client2",
					reqs: []req{
						{
							isGet:    true,
							key:      "key3",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key3",
							value:    "value3",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key4",
							value:    "",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key3",
							value:    "",
							expected: "value3",
						},
					},
				},
			},
		},
		{
			name:       "Complicated test with 3 clients",
			dirname:    "complicated-3clients/",
			numStorage: 4,
			requests: []clientReqs{
				{
					clientID: "client1",
					reqs: []req{
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value1",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value1",
						},
						{
							isGet:    false,
							key:      "key1",
							value:    "value2",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key1",
							value:    "",
							expected: "value2",
						},
					},
				},
				{
					clientID: "client2",
					reqs: []req{
						{
							isGet:    true,
							key:      "key3",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key3",
							value:    "value3",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key4",
							value:    "",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key3",
							value:    "",
							expected: "value3",
						},
					},
				},
				{
					clientID: "client3",
					reqs: []req{
						{
							isGet:    true,
							key:      "key4",
							value:    "",
							expected: "",
						},
						{
							isGet:    false,
							key:      "key4",
							value:    "value4",
							expected: "",
						},
						{
							isGet:    true,
							key:      "key4",
							value:    "",
							expected: "value4",
						},
						{
							isGet:    true,
							key:      "key4",
							value:    "",
							expected: "value4",
						},
					},
				},
			},
		},
	}
	// var r []req
	// for i := 0; i < 50; i++ {
	// 	req := req{
	// 		isGet:    i%5 == 0,
	// 		key:      strconv.Itoa(i % 3),
	// 		value:    strconv.Itoa(i),
	// 		expected: "",
	// 	}
	// 	r = append(r, req)
	// }

	// testCase := testCase{
	// 	name:       "Very big tests",
	// 	dirname:    "too-big/",
	// 	numStorage: 6,
	// 	requests: []clientReqs{
	// 		{
	// 			clientID: "client1",
	// 			reqs:     r,
	// 		},
	// 		{
	// 			clientID: "client2",
	// 			reqs:     r,
	// 		},
	// 		{
	// 			clientID: "client3",
	// 			reqs:     r,
	// 		},
	// 		{
	// 			clientID: "client4",
	// 			reqs:     r,
	// 		},
	// 		{
	// 			clientID: "client5",
	// 			reqs:     r,
	// 		},
	// 	},
	// }
	return testCases
}

func TestPredictable(t *testing.T) {
	os.RemoveAll(testoutDir)
	for _, test := range getTestCases() {
		t.Run(test.name, func(t *testing.T) {
			testdir := testoutDir + test.dirname
			defer runGrader(t, test.requests, test.numStorage, testdir)
			procs := setupKVSProcs(test.numStorage, testdir, t)
			defer cleanup(t, procs)
			runTestCase(t, test, nil, time.Second*0)
		})
	}
}

func TestChaos(t *testing.T) {
	// We test the system under circumstances where the storage node is failing at unpredictable, but reasonable times
	// when running this test, please make sure you run it multiple times, to simulate many scenarios
	os.RemoveAll(testoutDir)
	for _, test := range getTestCases() {
		t.Run(test.name, func(t *testing.T) {
			testdir := testoutDir + test.dirname
			defer runGrader(t, test.requests, test.numStorage, testdir)
			procs, killchan, mtx := setupKVSProcsChaos(test.numStorage, testdir, t) // We set a default number of restarts, we will keep repeating the test case
			// until the storage node has restarted this many times.
			// this is because a storage node restarting is very slow (as is expected)
			// and we would typically be done sending requests way before the process is restarted
			// so by adding this, we ensure that we experience the restart
			defer func() {
				killchan <- struct{}{} // this kills the storage node perminantely
				cleanup(t, procs)
			}()
			runTestCase(t, test, mtx, CHAOSINTERVAL)
		})
	}
}

func runTestCase(t *testing.T, test testCase, requestMtx *sync.Mutex, sleepInterval time.Duration) {
	wg := &sync.WaitGroup{}
	wg.Add(len(test.requests))
	errchan := make(chan error)
	donechan := make(chan struct{})
	for i := range test.requests {
		go func(i int) {
			var config ClientConfig
			err := ReadJSONConfig("config/client_config.json", &config)
			if err != nil {
				errchan <- err
				return
			}
			clientReqs := test.requests[i]
			config.ClientID = clientReqs.clientID
			client := NewClient(config, kvslib.NewKVS())
			if err := client.Initialize(); err != nil {
				errchan <- fmt.Errorf("Failed to initialize client: %v", err)
				return
			}
			clientdoneChan := make(chan struct{})
			go func() {
				for range clientReqs.reqs {
					<-client.NotifyChannel
				}
				clientdoneChan <- struct{}{}
			}()
			for _, request := range clientReqs.reqs {
				// we sleep between requests, to allow the storage node to fail
				time.Sleep(sleepInterval)
				if requestMtx != nil {
					requestMtx.Lock() // we lock and unlock right away, so that if the
					// storage is dying, we can wait for it, but we allow it to die while
					// processiong requests
					requestMtx.Unlock()
				}
				if request.isGet {
					_, err = client.Get(clientReqs.clientID, request.key)
				} else {
					_, err = client.Put(clientReqs.clientID, request.key, request.value)
				}
				if err != nil {
					errchan <- fmt.Errorf("Get request returned an error: %v", err)
					return
				}
			}
			<-clientdoneChan
			client.Close()
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		donechan <- struct{}{}
	}()
	select {
	case <-donechan:
		return
	case err := <-errchan:
		t.Fatalf("Error running test: %v", err)
	}
}

func setupKVSProcs(numStorageProcs int, testPath string, t *testing.T) []*kvsProcess {
	os.MkdirAll(testPath, 0755)
	tracingProc, err := startProc(testPath+"tracer-out.log", "go", "run", "cmd/tracing-server/main.go")
	if err != nil {
		t.Fatalf("Failed to run the tracing serve")
	}
	frontendProc, err := startProc(testPath+"frontend-out.log", "go", "run", "cmd/frontend/main.go")
	if err != nil {
		t.Fatalf("Failed to run the frontned")
	}
	var storageProcs []*kvsProcess
	for i := 0; i < numStorageProcs; i++ {
		storageNum := strconv.Itoa(i)
		storageProc, err := startProc(testPath+"storage"+storageNum+".log", "go", "run", "cmd/storage/main.go", "--id",
			"storage"+storageNum, "--add", ":"+strconv.Itoa(int(genPort())), "--disk", "diskpaths/"+"storage"+storageNum+"/")
		if err != nil {
			t.Fatalf("Failed to run the storage")
		}
		storageProcs = append(storageProcs, storageProc)
	}
	var config ClientConfig
	err = ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		t.Fatal(err)
	}
	return append([]*kvsProcess{tracingProc, frontendProc}, storageProcs...)
}

func cleanupDisk(t *testing.T) {
	var config StorageConfig
	if err := ReadJSONConfig("config/storage_config.json", &config); err != nil {
		t.Fatalf("Failed to read storage config: %v", err)
	}
	if err := os.RemoveAll(config.DiskPath); err != nil {
		t.Fatalf("Failed to remove the diskpath content: %v", err)
	}
}

func runGrader(t *testing.T, requests []clientReqs, numStroage int, gradePath string) {
	smokeTestargs := getSmokeTestArgs(requests, numStroage)
	log.Printf("Args are: %v", smokeTestargs)
	cmd := exec.Command("./gradera6-smoketest", smokeTestargs...)
	// NOTE: IF we have GIGANTIC tests, out may be too big to fit into memory...
	// but until we hit that case, that works - we can fix it with buffering our stdout
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("Failed to run the smoke test: %v", err)
	}
	ioutil.WriteFile(gradePath+"grade.log", out, 0755)

	// ditto the above note, it works for now so we keep it
	trace, err := ioutil.ReadFile("trace_output.log")
	if err != nil {
		t.Fatalf("Unable to read the trace file: %v", err)
	}
	ioutil.WriteFile(gradePath+"trace_output.log", trace, 0755)
	shiviz, err := ioutil.ReadFile("shiviz_output.log")
	if err != nil {
		t.Fatalf("Unable to read the shiviz file: %v", err)

	}
	ioutil.WriteFile(gradePath+"shiviz_output.log", shiviz, 0755)
	checkGrade(t, gradePath+"grade.log")
}

func getSmokeTestArgs(requests []clientReqs, numStorage int) []string {
	args := []string{"-t", "trace_output.log"}
	clientMap := map[string]bool{}
	reqs := map[string]int{}
	for _, clientReq := range requests {
		_, ok := clientMap[clientReq.clientID]
		if !ok {
			args = append(args, "-c")
			args = append(args, clientReq.clientID)
		}
		for _, req := range clientReq.reqs {
			reqStr := clientReq.clientID
			if req.isGet {
				reqStr = reqStr + ":get"
			} else {
				reqStr = reqStr + ":put"
			}
			reqStr = reqStr + ":" + req.key
			count, ok := reqs[reqStr]
			if !ok {
				reqs[reqStr] = 1
			} else {
				reqs[reqStr] = count + 1
			}
		}
	}
	for key, value := range reqs {
		args = append(args, "-r")
		req := strings.Split(key, ":")
		res := []string{req[0], req[1], strconv.Itoa(value), req[2]}
		args = append(args, strings.Join(res, ":"))
	}
	for i := 0; i < numStorage; i++ {
		args = append(args, "-s")
		args = append(args, "storage"+strconv.Itoa(i))
	}
	return args
}

func checkGrade(t *testing.T, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Unable to open file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for i := 0; i < 3; i++ {
		scanner.Scan() // first three lines in the grade file are irrelevant
	}
	scanner.Scan()
	line := scanner.Text()
	split := strings.Split(line, " ")
	grade := split[len(split)-1]
	split = strings.Split(grade, "/")
	if split[0] != split[1] {
		t.Fatalf("Did not get full marks, please check %s. Got %s out of %s", filename, split[0], split[1])
	}
	log.Printf("Good job, got %s, out of %s", split[0], split[1])
}

// Kills all processes passed in.
// in the case processes don't get cleaned up (the tests recieve an interrupting signal to terminate, like CTRL-C), you can run the following unix bash script:
//	$ ps aux | grep go | awk '{print $2}' | xargs kill -9
// which kills any lingering go processes
type kvsProcess struct {
	name    string
	cmd     *exec.Cmd
	logFile *os.File
}

func cleanup(t *testing.T, processes []*kvsProcess) {
	for _, p := range processes {
		killProc(p)
	}
	cleanupDisk(t)
}

func genPort() int32 {
	return rand.Int31n(35535-1024) + 1024
}

func startProc(logfilePath string, name string, args ...string) (*kvsProcess, error) {
	cmd := exec.Command(name, args...)
	logFile, err := os.OpenFile(logfilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	time.Sleep(time.Second * 2)
	return &kvsProcess{cmd: cmd, logFile: logFile}, nil
}

// our failure mechanism, we kill the process to represent storage node failures
func killProc(process *kvsProcess) {
	process.logFile.Close()
	pgid, err := syscall.Getpgid(process.cmd.Process.Pid)
	if err == nil {
		syscall.Kill(-pgid, 15)
	}
}

const CHAOSINTERVAL time.Duration = time.Millisecond * 200

func setupKVSProcsChaos(numStorage int, testPath string, t *testing.T) ([]*kvsProcess, chan<- struct{}, *sync.Mutex) {
	os.MkdirAll(testPath, 0755)
	tracingProc, err := startProc(testPath+"tracer-out.log", "go", "run", "cmd/tracing-server/main.go")
	if err != nil {
		t.Fatalf("Failed to run the tracing serve")
	}
	frontendProc, err := startProc(testPath+"frontend-out.log", "go", "run", "cmd/frontend/main.go")
	if err != nil {
		t.Fatalf("Failed to run the frontned")
	}
	donechan := make(chan struct{})
	mtx, _ := chaos(numStorage, testPath, donechan, CHAOSINTERVAL)
	var config ClientConfig
	err = ReadJSONConfig("config/client_config.json", &config)
	if err != nil {
		t.Fatal(err)
	}
	return []*kvsProcess{tracingProc, frontendProc}, donechan, mtx
}

// Introduce absolute chaos to the storage node. This way we can stress test that storage node failures
// do not end up destroying our system.
// NOTE: tests that consume this function SHOULD NOT assume an expected passing or failure or requests
// but should expect that they recieve responses that satisfy monotonic reads and writes
// In other words, regardless if we are in absolute chaos, requests that pass, should be reflected on
// our persistant storage
// # Arguments
// 	- timeTillDead: Time until we kill the node perminantely, we can't assume that it will die exactly at this
// interval, however, calling nodes can use this variable as a reasonable estimate
//  - chaosInterval: The interval to keep the process alive. For example, if chaosInterval is 2 seconds, then the
// 					the storage node will be killed each two seconds
//  - name: The name of the command
// 	- args: The arguments to the command
// # Returns
// returns an error if something went wrong, callers should report a test failure
func chaos(numStorage int, testPath string, donechan <-chan struct{}, chaosInterval time.Duration) (*sync.Mutex, error) {
	var storageProcs []*kvsProcess
	for i := 0; i < numStorage; i++ {
		storageNum := strconv.Itoa(i)
		procArgs := []string{"run", "cmd/storage/main.go", "--id",
			"storage" + storageNum, "--add", ":" + strconv.Itoa(int(genPort())), "--disk", "diskpaths/" + "storage" + storageNum + "/"}
		cmd, err := startProc(testPath+"storage"+storageNum+".log", "go", procArgs...)
		if err != nil {
			return nil, err
		}
		storageProcs = append(storageProcs, cmd)
	}
	mtx := &sync.Mutex{}
	rand.Seed(time.Now().UnixNano())
	go func() {
		for {
			time.Sleep(CHAOSINTERVAL)
			select {
			case <-donechan:
				// We should permanently kill the process
				for _, proc := range storageProcs {
					killProc(proc)
				}
				log.Println("Storage processes killed successfully")
				return
			default:
				//we kill the process and restart it
				mtx.Lock()
				log.Println("Timer! storage nodes about to restart")
				left := rand.Int() % numStorage
				right := rand.Int() % numStorage
				if left == right {
					right = (right - 1) % numStorage
					if right == -1 {
						right = 0
					}
				}

				for i := left; i != right; i = (i + 1) % numStorage {
					log.Println("right " +  fmt.Sprint(right))
					log.Printf("Killing storage id: %d\n", i)
					killProc(storageProcs[i])
				}
				for i := left; i != right; i = (i + 1) % numStorage {
					log.Printf("Restarting storage with id: %d\n", i)

					// TODO: figure out how to handle an error in starting the process
					storageNum := strconv.Itoa(i)
					procArgs := []string{"run", "cmd/storage/main.go", "--id",
						"storage" + storageNum, "--add", ":" + strconv.Itoa(int(genPort())), "--disk", "diskpaths/" + "storage" + storageNum + "/"}
					cmd, err := startProc(testPath+"storage"+storageNum+".log", "go", procArgs...)
					if err != nil {
						log.Printf("Timer! Process has been restarted, but we saw an issue: %v", err)
					}
					storageProcs[i] = cmd
				}
				log.Printf("Timer! Process has been restarted successfully")
				mtx.Unlock()
			}
		}
	}()
	return mtx, nil
}