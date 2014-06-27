package main

import (
    "fmt"
	"encoding/csv"
	"os"
	"io"
	"path"
	"path/filepath"
	"log"
	"time"
	"strings"
	"sort"
	"strconv"
	"crypto"
	"github.com/eclesh/hyperloglog"
)

// Structure to hold filenames to be ordered by the time part
type Event struct {
	path string
	ts time.Time	
}

type Events []Event

// Declare methods needed to sort implementation on events
//
func (this Events) Len() int {
    return len(this)
}
func (this Events) Less(i, j int) bool {
    return this[i].ts.Unix() < this[j].ts.Unix()
}
func (this Events) Swap(i, j int) {
    this[i], this[j] = this[j], this[i]
}


// Structure to hold Map of HLL and time
type CellMap struct {
	f string
	m map[string]*hyperloglog.HyperLogLog
}

// Dum error checking function

func check(e error) {
    if e != nil {
        panic(e)
    }
}

// Function called to create a summary file for Key with same Timestamp 
// and then delete all those eys from DB

func searchAndDestroy(cm *CellMap) {
	
	// Open output file for Key
	
		filename := fmt.Sprintf("/Volumes/BigBud/data/vf/results/%s.csv", cm.f)
	   	file, err := os.Create(filename)
	   	check(err)
	   	defer file.Close()
	
		// Write Headers
	    _, err = file.WriteString("Cell, Count\n")
	
		// Loop on the map of hll to get count for cell
		for key, value := range cm.m {
		    _, err = file.WriteString(fmt.Sprintf("%s, %d\n", key, value.Count()))
			check(err)
		}
		// ToDo: should we delete the structure???
		// delete cm
		// Issue a Sync to flush writes to stable storage.
	    file.Sync()
	
	
	// Scan the map of HLL for the set and write down the values
	// Write line with count computed for cell by Hyperloglog
	// Delete keys

}


// Function listen for Timestamps ready to be summarized
// and then cleaned-up

func listenUp(channel chan *CellMap, count int) {
	
	for key := range channel {
		searchAndDestroy(key)
	}
} 

// Read all the files names in all directories from provided root
// Return an ordered list of file names to process

func processFile(inFile string, cm *CellMap) error {
		
//	println(">> Processing: ", inFile)
	file, err := os.Open(inFile) // For read access.
	
	if err != nil {
		if err == os.ErrNotExist {
			log.Fatal("No such file!")
		} else {
			log.Fatal(err)
		}
	}
	defer file.Close()

	r := csv.NewReader(file)
		
	// Read First Line
	row, err := r.Read()
	
	i := 0
	
	var count int64
	
	// Get ride of the unused variable error when printf is commented!
	var _ = count
	
	count = 0
	var pipeCount float64 = 0
	var tt time.Time

	startTime := time.Now()
	fmt.Printf("%s - %s Procesing starts ", startTime.Format("2006/01/2 - 15:04:05"), inFile)
 	
	for (err != io.EOF) && (len(row) >1) {

		// Add imsi value for that key (Time:Cell) value to Redis
		// Create a set per key with all the cell for that period
		
		if pipeCount == 0 {
		// Optimize in the case of 5mins files take the time of the first line (rounded)
		// This will break if files contain more than 5 mins splits
			utime, _ := strconv.ParseInt(row[1], 10, 64)
			t := time.Unix(utime, 0).In(time.UTC)
			tt = t.Truncate(5 * time.Minute)	
			_ = tt
//			set = fmt.Sprintf("set:%d", tt.Unix())
		}

		key := row[3] // Cell
		imsi, _ := strconv.ParseUint(row[2], 10, 64)
		hll, ok := cm.m[key]
		if ok {
			hll.Add(uint32(imsi))	
		} else { // not found
			hll, err := hyperloglog.New(8192)
			check(err)
			hll.Add(uint32(imsi))
			fmt.Printf("add Imsi: %d - %d\n", imsi, uint32(imsi))
			cm.m[key] = hll	
		}

		pipeCount += 1

		// Next line
     	row, err = r.Read()
		i += 1 
	}
	
	endTime := time.Now()
	duration := endTime.Sub(startTime)
		
 	if err != nil && err != io.EOF {
   		fmt.Println("Error:", err)
 	}
    fmt.Print(" lasted: ", duration)
	fmt.Printf(" for %d keys inserted\n", i)

	return nil
}

// Main function
func main() {
	
// Create Channel for inter routine communication
	channel := make (chan *CellMap, 10)

	
	// Create list of file to be processed
	eList := make(Events, 0, 100)
	
	// Launch 5 processing functions
	for i :=0; i < 5; i++ {
		go listenUp(channel, i)
	}
	 
	// Recursive walk to get all the csv files
	filepath.Walk("/Volumes/BigBud/data/vf", func(aPath string, info os.FileInfo, err error) error {
		// fmt.Println(path)
		if (!info.IsDir() && strings.HasSuffix(info.Name(), ".csv")) {
			slice := strings.Split(path.Base(aPath), ".")
			sDate := slice[0] +":"+ slice[1]
			t, err := time.ParseInLocation("20060102:150405", sDate, time.UTC)
			if err != nil {
				fmt.Println(err)
// Avoid fatal on result file name that has different structure
//				log.Fatal("Abort: Bad Filename with date: ", sDate)
			} else {
				e := Event{aPath, t}
				eList = append(eList, e)				
			}

		}
		return nil
	})

	// Order the events by date
	sort.Sort(eList)
	
	// Loop thru the files, ordered by time and file the unique counts
	// Breaks on 5 min boundaries to geterate cumulated result file and purge redis for those keys
	// Need to slow down ingestion process if too much data being inserted
	
	var count int = 0
	
	startTime := time.Now()
	fmt.Println("Let's get started: ", startTime)	
	
	// Initialize first timestamp
	
	var pTime time.Time
	
	var cm *CellMap = nil
 
	if eList.Len() > 0 {
		pTime = eList[0].ts
		cm = new(CellMap)
		slice := strings.Split(path.Base(eList[0].path), ".")
		sDate := slice[0] +"-"+ slice[1]
		cm.f = sDate
		cm.m = make(map[string]*hyperloglog.HyperLogLog)
	}
		
	for _, v := range eList {

		if pTime != v.ts {
			fmt.Printf("Process time %s with key: %d\n", pTime, pTime.Unix())
			channel <- cm
			pTime = v.ts
			// Initialize new cm for next batch of 5 min
			cm = nil
			cm = new(CellMap)
			slice := strings.Split(path.Base(v.path), ".")
			sDate := slice[0] +"-"+ slice[1]
			cm.f = sDate
			cm.m = make(map[string]*hyperloglog.HyperLogLog)
		}		
				
		err := processFile(v.path, cm)
		
		
		if err != nil {
			log.Fatal(err)
		}
		// For test only
		if count += 1; count > 10000 {
			break
		}
	}

// Close all and Cleanup
	close(channel)
		
	println("Total File List ", eList.Len())

	endTime := time.Now()
	duration := endTime.Sub(startTime)

    fmt.Println("Let's end: ", duration)
		
	os.Exit(0)
}