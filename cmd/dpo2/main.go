package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"imdea.org/redbelly/configuration"
	"imdea.org/redbelly/dpo"
	"imdea.org/redbelly/dso"
	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/p2p"
	"imdea.org/redbelly/p2p/zeromq"
	"imdea.org/redbelly/types"
)

var logger *logging.Logger

var nodeID int
var peersListPath string
var listeningPort int
var logConf logging.LogConf
var nTX int
var totalTX int
var epochIncPeriod int
var benchmarkCSV string
var finalNumbersCSV string
var broadcastType int
var broadcastPeriod int
var automaticEpochInc bool
var silent int
var addToFPlus1 bool
var duration int
var getPeriod int
var dsoServer string
var dsoServersFile string
var txPeriod int

var nodeIDString string
var n uint
var f uint

var starting_time time.Time

func init() {
	flag.IntVar(&nodeID, "id", -1, "Index for the node")
	flag.StringVar(&peersListPath, "peers", "", "Path to the peers file")
	flag.IntVar(&listeningPort, "l", 4000, "Port for incoming peer connections")
	flag.IntVar(&nTX, "ntx", 0, "Number of test txs to send per second")
	flag.IntVar(&txPeriod, "txPeriod", 1000, "Sets period to ad transactions")
	flag.IntVar(&totalTX, "ttx", 0, "Total number of transactions to generate")
	flag.IntVar(&epochIncPeriod, "epochPeriod", 0, "Sets a periodic epoch increment request")
	flag.StringVar(&benchmarkCSV, "benchmark", "", "The output file to save benchmark statistics to")
	flag.StringVar(&finalNumbersCSV, "finalNumbers", "", "The output file to save benchmark statistics to")
	flag.IntVar(&broadcastPeriod, "broadcastPeriod", 0, "Sets a periodic epoch increment request")
	flag.IntVar(&broadcastType, "broadcastType", 0, "Broadcast type: 0 = Immediate, 1 = PeriodicBroadcast, 2 = AfterEpoch")
	flag.BoolVar(&automaticEpochInc, "automaticEpochInc", false, "Whether after an EpochInc finishes the server starts a new one")
	flag.IntVar(&silent, "silent", 0, "Number of silent servers. Must be less than n/3")
	flag.IntVar(&duration, "d", 0, "Sets the test duration (in Seconds)")
	flag.IntVar(&getPeriod, "getPeriod", 0, "Sets the period to get info (in Seconds)")
	flag.StringVar(&dsoServer, "dso", "", "The connection string for a DSO server")
	flag.StringVar(&dsoServersFile, "dsoFile", "", "The connection string for a DSO server")
	flag.BoolVar(&addToFPlus1, "addF1", false, "")
	flag.Var(&logConf, "log", "Configure log for a component")
	flag.Parse()

	nodeIDString = fmt.Sprintf("Node%d", nodeID)

	for _, lc := range logConf {
		componentLog := strings.Split(lc, ":")
		logLevel, err := strconv.Atoi(componentLog[1])
		if err != nil || len(componentLog[0]) == 0 {
			log.Fatal("Can't recognize log level")
		}
		logging.LogLevels[componentLog[0]] = logLevel
	}

	logger = logging.NewLogger(nodeIDString)
	if nodeID == -1 {
		logger.Error.Fatalln("A node id must be provided. (-id)")
	}
	if peersListPath == "" {
		logger.Error.Fatalln("A peers list must be provided. (-peers)")
	}
}

func main() {
	var err error
	backgroundCtx := context.Background()
	mainCtx, mainCancel := context.WithCancel(backgroundCtx)

	if benchmarkCSV != "" {
		err := initBenchmark(benchmarkCSV, nodeIDString)
		if err != nil {
			logger.Error.Fatalln("Can't configure benchmark output")
		}
		benchmarkCtx, benchmarkCancel := context.WithCancel(mainCtx)
		defer benchmarkCancel()
		dpo.BML.Start(benchmarkCtx)
	}

	peerList, err := configuration.PeerListFromJsonFile(peersListPath)
	if err != nil {
		logger.Error.Fatalln("Error reading the peer list: ", err)
	}

	n = uint(peerList.Count())
	f = (n - 1) / 3

	if uint(silent) > f {
		logger.Error.Fatalln("Number of silent nodes too big. Must be less or equal than ", f)
	}
	behaviour := types.Correct
	if nodeID < silent {
		behaviour = types.Silent
	}

	//Starts DSO
	go dsoRunner()
	var dsoClient dso.IDSO

	if dsoServer != "" && dsoServersFile != "" {
		logger.Error.Fatalln("Can't use dso file and dso server string together")
	}
	if dsoServer == "" && dsoServersFile == "" {
		logger.Error.Fatalln("A DSO configuration must be set")
	}

	logger.Info.Println("Connecting to DSO")

	if dsoServer != "" {
		client := dso.NewDSOClient()
		attempts := 0
		for {
			if client.Connect(dsoServer) == nil {
				dsoClient = client
				break
			}
			attempts++
			if attempts == 10 {
				logger.Error.Fatalln("Exiting after failing to connect to DSO")
			}
			time.Sleep(3 * time.Second)
		}
		logger.Info.Println("Connected to DSO at", dsoServer)
	} else {
		client := dso.NewWDSOClient(int(f))
		serversList, err := configuration.PeerListFromJsonFile(dsoServersFile)
		if err != nil {
			logger.Error.Fatalln("Invalid DSO Servers file")
		}
		attempts := 0
		for {
			if client.Connect(*serversList) == nil {
				dsoClient = client
				break
			}
			attempts++
			if attempts == 10 {
				logger.Error.Fatalln("Exiting after failing to connect to DSO")
			}
			time.Sleep(3 * time.Second)
		}
		logger.Info.Println("Connected to DSO at", serversList)
	}

	ns := zeromq.NewZMQUNetworkService(
		nodeID,
		peerList.Count(),
		*peerList,
		fmt.Sprintf("tcp://*:%d", listeningPort),
		behaviour,
	)
	ns.Start()

	<-ns.PingAll()

	router := p2p.NewRouter(ns)
	router.Start()
	defer router.Stop()

	proposer, _ := types.NewProposerIdentity(types.ProposerIndex(nodeID))

	dpo := dpo.NewDPO_2(router, n, f, proposer, types.VerifySignedTransaction, automaticEpochInc, dsoClient)

	dpo.Start()
	defer dpo.Stop()
	starting_time = time.Now()

	if nTX != 0 {
		adderCtx, adderCancel := context.WithCancel(mainCtx)
		defer adderCancel()
		if !addToFPlus1 {
			go constantMempoolFiller(adderCtx, dpo, time.Duration(txPeriod)*time.Millisecond, nTX)
		} else {
			go simulateAddToFPlus1Servers(adderCtx, dpo, 1000*time.Millisecond, nTX, peerList.Count(), (peerList.Count()-1)/3)
		}
	}

	if epochIncPeriod != 0 {
		epochIncCtx, epochIncCancel := context.WithCancel(mainCtx)
		defer epochIncCancel()
		go periodicEpochIncrement(epochIncCtx, dpo, time.Duration(epochIncPeriod)*time.Millisecond)
	}

	if getPeriod != 0 {
		getterCtx, getterCancel := context.WithCancel(mainCtx)
		defer getterCancel()
		go periodicGetterClient(getterCtx, dpo, time.Duration(getPeriod)*time.Second)
		if finalNumbersCSV != "" {
			go getPeriodicFinalNumbers(dpo, finalNumbersCSV, time.Duration(getPeriod)*time.Second)
		}
	}

	if duration == 0 {
		stopChan := make(chan os.Signal, 1)
		signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
		<-stopChan
	} else {
		time.Sleep(time.Duration(duration) * time.Second)
	}

	if finalNumbersCSV != "" {
		getFinalNumbers(dpo, finalNumbersCSV)
	}
	mainCancel()
	logger.Info.Println("Stopping DPO")
}

// test function to fulfill mempool
func randomAdderClient(ctx context.Context, dpo *dpo.DPO) {
	_, sk, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		logger.Error.Println("Error generating client keys: ", err)
	}
loop:
	for {
		select {
		default:
			val, _ := rand.Int(rand.Reader, big.NewInt(1000000))
			tx := types.Transaction(fmt.Sprintf("Record%s", val.String()))
			signed_tx := tx.Sign(sk)
			added, err := dpo.Add(signed_tx)
			if err != nil {
				logger.Error.Println(err)
			} else if !added {
				logger.Info.Println("Discarded already present record")
			}
			time.Sleep(time.Duration(val.Int64()) * time.Microsecond)
		case <-ctx.Done():
			break loop
		}
	}
}

// test function to fulfill mempool
func constantMempoolFiller(ctx context.Context, dpo *dpo.DPO_2, p time.Duration, q int) {
	ticker := time.NewTicker(p)
	_, sk, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		logger.Error.Printf("Error generating client keys: %v\n", err)
	}
	totalGeneratedTransactions := 0
loop:
	for {
		select {
		case <-ticker.C:
			for i := 0; i < q; i++ {
				tx := types.Transaction(time.Now().Format(time.RFC3339Nano))
				signed_tx := tx.Sign(sk)
				added, err := dpo.Add(signed_tx)
				if err != nil {
					logger.Error.Println(err)
				} else if !added {
					logger.Info.Println("Discarded already present record")
				}
			}
			totalGeneratedTransactions += q
			if totalTX != 0 && totalGeneratedTransactions >= totalTX {
				ticker.Stop()
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}
}

//It does not actually contact f+1 servers but it adds to server i records v*n+(i+j)%n with j \in [0,f].
//Then, if all clients implement this function, is equivalent to add each record to f+1 servers.
func simulateAddToFPlus1Servers(ctx context.Context, dpo *dpo.DPO_2, p time.Duration, q int, n int, f int) {
	ticker := time.NewTicker(p)
	_, sk, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		logger.Error.Printf("Error generating client keys: %v\n", err)
	}
	totalGeneratedTransactions := 0
	val := 0
loop:
	for {
		select {
		case <-ticker.C:
			for i := 0; i < q; i++ {
				for j := 0; j < f+1; j++ {
					offset := (j + nodeID) % n
					tx := types.Transaction(fmt.Sprintf("Record%d", val+offset))
					signed_tx := tx.Sign(sk)
					added, err := dpo.Add(signed_tx)
					if err != nil {
						logger.Error.Println(err)
					} else if !added {
						logger.Info.Println("Discarded already present record ", val+offset)
					}
				}
				val += n
			}
			totalGeneratedTransactions += q
			if totalTX != 0 && totalGeneratedTransactions >= totalTX {
				ticker.Stop()
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}
}

func periodicGetterClient(ctx context.Context, d dpo.IDPO, p time.Duration) {
	var storage *dpo.Storage
	// var currentEpoch int
loop:
	for {
		select {
		default:
			storage, _ = d.Get()
			// if currentEpoch < int(storage.Epoch) {
			latestEpoch := storage.History[storage.Epoch]
			logger.Info.Printf("Latest Epoch: %d N.TXs: %d\n", storage.Epoch, len(latestEpoch))
			// for k := range latestEpoch {
			// 	logger.Info.Println(k[96:])
			// }
			logger.Info.Println("Current set:", storage.Set.Size())
			// for k := range storage.Set {
			// 	logger.Info.Println(k[96:])
			// }
			// 	currentEpoch = int(storage.Epoch)
			// }
			time.Sleep(p)
		case <-ctx.Done():
			break loop
		}
	}
}

func periodicEpochIncrement(ctx context.Context, dpo *dpo.DPO_2, p time.Duration) {
	var err error
loop:
	for {
		select {
		default:
			time.Sleep(p)
			nextEpoch := dpo.GetNextEpoch()
			err = dpo.EpochInc(nextEpoch)
			if err != nil {
				logger.Error.Println(err)
				logger.Error.Printf("Failed to send EpochInc to %d\n", nextEpoch)
			}
		case <-ctx.Done():
			break loop
		}
	}
}

func initBenchmark(fileName string, identifier string) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	dpo.BML, err = logging.NewBenchmarkLogger(f, identifier)
	if err != nil {
		return err
	}
	return nil
}

func getFinalNumbers(dpo *dpo.DPO_2, fileName string) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Error.Println("cannot open file", err)
		return
	}
	writer := csv.NewWriter(f)
	fInfo, _ := f.Stat()
	if fInfo.Size() == 0 {
		writer.Write([]string{
			"# elements added",
			"# elements added by client",
			"# elements setBroadcasted",
			"# elements stamped",
			"# elements added by epochInc",
			"nextEpoch",
			"duration (seconds)",
			"nodeId",
			"# nodes",
			"ntx",
			"totalTx",
			"epoch period",
			"broadcastType",
			"broadcast period",
			"automatic EpochInc",
			"silent",
			"add to f+1",
		})
		writer.Flush()
		err = writer.Error()
		if err != nil {
			logger.Error.Printf("Failed to write log to file: %s", err)
		}
	}
	writer.Write([]string{
		fmt.Sprint(dpo.GetTotalElementsAdded()),
		fmt.Sprint(dpo.GetTotalAddedByClient()),
		fmt.Sprint(dpo.GetTotalSetBroadcasted()),
		fmt.Sprint(dpo.GetTotalElementsStamped()),
		fmt.Sprint(dpo.GetTotalAddedByEpochInc()),
		fmt.Sprint(dpo.GetNextEpoch()),
		fmt.Sprint(time.Since(starting_time)),
		nodeIDString,
		fmt.Sprint(n),
		fmt.Sprint(nTX),
		fmt.Sprint(totalTX),
		fmt.Sprint(epochIncPeriod),
		fmt.Sprint(types.BroadcastType(broadcastType)),
		fmt.Sprint(broadcastPeriod),
		fmt.Sprint(automaticEpochInc),
		fmt.Sprint(silent),
		fmt.Sprint(addToFPlus1),
	})
	writer.Flush()
	err = writer.Error()
	if err != nil {
		logger.Error.Printf("Failed to write log to file: %s", err)
	}
}

func getPeriodicFinalNumbers(dpo *dpo.DPO_2, fileName string, p time.Duration) {
	ticker := time.NewTicker(p)
	for {
		<-ticker.C
		getFinalNumbers(dpo, fileName)
	}
}

func dsoRunner() {
	dsoCMD := exec.Command("/dpo/dso", "-id", fmt.Sprint(nodeID), "-peers", "/dpo/dso.json", "-log", fmt.Sprintf("DSONode%d:4", nodeID), "-silent", fmt.Sprint(silent), "-broadcastPeriod", fmt.Sprint(broadcastPeriod))
	dsoCMD.Stdout = os.Stdout
	dsoCMD.Stderr = os.Stderr
	err := dsoCMD.Run()
	if err != nil {
		logger.Error.Println(err)
	}
	logger.Info.Println("DSO Stopped")
}
