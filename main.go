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
	"github.com/garyburd/redigo/redis"
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


// Dum error checking function

func check(e error) {
    if e != nil {
        panic(e)
    }
}

// Function called to create a summary file for Key with same Timestamp 
// and then delete all those eys from DB

func searchAndDestroy(key int64) {
	
	// Open output file for Key
	
		filename := fmt.Sprintf("/Volumes/BigBud/data/vf/results/%d.txt", key)
	   	file, err := os.Create(filename)
	   	check(err)
	   	defer file.Close()
	
		// Write Headers
	    _, err = file.WriteString("Cell, Count\n")
	
		// Issue a Sync to flush writes to stable storage.
	    file.Sync()
	
	// Get Connection from the pool. Only way to be thread-safe with Redis
		c := pool.Get()	
		if c == nil {
				log.Fatal("Cannot get connection from Redis Pool")
		}
		defer c.Close()
	
	// Scan the database to find all entries starting with provided key
	// Write line with count computed for cell by Hyperloglog
	// Delete keys
	
	/*
			s, err := redis.String(c.Do("scan", key))

			count, err = redis.Uint64(cnx.Do("PFCOUNT", key))		
			if err != nil {
				fmt.Println("Can't find key: " + key)			fmt.Println("Error: ")
			} else {
				fmt.Printf("Redis: %s = %d\n", key, count)
			}
	*/		
}


// Function listen for Timestamps ready to be summarized
// and then cleaned-up

func listenUp(channel chan int64, count int) {
	
	for key := range channel {
		searchAndDestroy(key)
	}
} 

// Read all the files names in all directories from provided root
// Return an ordered list of file names to process

func processFile(inFile string) error {

// Get Connection from the pool. Only way to be thread-safe with Redis
	c := pool.Get()	
	if c == nil {
			log.Fatal("Cannot get connection from Redis Pool")
	}
	defer c.Close()
		
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
	
	startTime := time.Now()
	fmt.Printf("%s - %s Procesing starts ", startTime.Format("2006/01/2 - 15:04:05"), inFile)
 	
	for (err != io.EOF) && (len(row) >1) {
		
		// Could optimize in the case of 5mins files (always same round t)
		// But will break if process larger files
		
		utime, _ := strconv.ParseInt(row[1], 10, 64)
		
		t := time.Unix(utime, 0).In(time.UTC)
		
		// t, _ := time.Parse(time.RFC3339, row[0])
		
		// Could use Truncate to take lower interval
		tt := t.Truncate(5 * time.Minute)
	
		key := fmt.Sprintf("%d:%s", tt.Unix(), row[3])
		imsi := row[2]
				
//		fmt.Printf("     %d - Time=%s Key=%s Imsi=%s\n", i, tt.String(), key, imsi)
		// Add imsi value for that key (Time:Cell) value to Redis
		c.Do("PFADD", key, imsi)
	
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

// Utility function to allocate pool

func newPool(server, password string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }
			if password != "" {
            	if _, err := c.Do("AUTH", password); err != nil {
                	c.Close()
                	return nil, err
            	}
			}
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

// Global variables

var (
    pool *redis.Pool
    redisServer = "localhost:6379"
    redisPassword = ""
)

// Main function
func main() {

// Create Channel for inter routine communication
	channel := make (chan int64, 10)
	
// Open Connection to Redis Server
//	const proto = "tcp"
//	const port = ":6379"
//	c, err := redis.Dial(proto, port)
	
	pool = newPool(redisServer, redisPassword)

	if pool == nil {
			log.Fatal("Cannot Create Redis Pool")
	}

// Get Connection from the pool. Only way to be thread-safe with Redis
	c := pool.Get()	
	if c == nil {
			log.Fatal("Cannot get connection from Redis Pool")
	}
	defer c.Close()
	
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
				log.Fatal("Abort: Bad Filename with date: ", sDate)
			}
			e := Event{aPath, t}
			eList = append(eList, e)
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
	
	if eList.Len() > 0 {
		pTime = eList[0].ts
	}
		
	for _, v := range eList {

		if pTime != v.ts {
			fmt.Printf("Process time %s with key: %d\n", pTime, pTime.Unix())
			channel <- pTime.Unix()
			pTime = v.ts
		}		
		
//		fmt.Printf ("Processing: %s - %s\n", v.ts, v.path)		
		err := processFile(v.path)
		
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