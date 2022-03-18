package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Version = "v0.0.4"

type Job struct {
	name    string
	cmd     string
	childs  []*Job
	state   JobState
	force   bool
	level   int
	uniqID  string
	started time.Time
	stopped time.Time
	err     error
}

type Worker struct {
	ID     string
	stopCh chan bool
	wg     *sync.WaitGroup
	wgInit *sync.WaitGroup
}

type Collector struct {
	ID     string
	stopCh chan bool
	wg     *sync.WaitGroup
	wgInit *sync.WaitGroup
}

type Scheduler struct {
	ID       string
	stopCh   chan bool
	sleep    bool
	sleepMux sync.Mutex
	wg       *sync.WaitGroup
	wgInit   *sync.WaitGroup
}

type JobQeue struct {
	jobs          []*Job
	jobsRemaining int
	totalJobs     int
	jobsFailed    int
	mux           sync.Mutex
}

type Context struct {
	workDir       string
	outDir        string
	inputFileSafe string
	inputSafe     string
	logDir        string
	inDir         string
	force         bool
}

type JobState int

const (
	JOB_NOT_STARTED JobState = 0 + iota
	JOB_DONE
	JOB_CACHED
)

func dummyJob(job *Job) {
	i, _ := strconv.Atoi(job.cmd)

	time.Sleep(time.Duration(rand.Intn(i)) * time.Millisecond)
}

func fileSafeString(str string) string {
	r, _ := regexp.Compile("[^A-Za-z0-9\\-]+")

	safeStr := r.ReplaceAllString(str, "-")

	return safeStr
}

func jobUniqID(job *Job) string {
	h := sha256.New()

	h.Write([]byte(job.name))
	h.Write([]byte(job.cmd))
	h.Write([]byte(strconv.Itoa(job.level)))

	sha256sum := h.Sum(nil)
	hex256sum := hex.EncodeToString(sha256sum)

	safeName := fileSafeString(job.name)

	return filepath.Clean(safeName + "-" + hex256sum[0:7])
}

func jobCached(ctx *Context, job *Job) bool {
	jobFile := filepath.Join(ctx.logDir, job.uniqID)

	_, err := os.Stat(jobFile)

	if os.IsNotExist(err) {
		return false
	}
	return true
}

func cacheJob(ctx *Context, job *Job, out []byte) {
	jobFile := filepath.Join(ctx.logDir, job.uniqID)

	if job.err != nil {
		jobFile = jobFile + ".err"
	}
	f, err := os.Create(jobFile)

	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString("name: " + job.name + "\n")
	f.WriteString("cmd: " + job.cmd + "\n")
	f.WriteString("level: " + strconv.Itoa(job.level) + "\n")
	f.WriteString("started at: " + job.started.String() + "\n")
	f.WriteString("stopped at: " + job.stopped.String() + "\n")
	if job.err != nil {
		f.WriteString("error: " + job.err.Error() + "\n")
	} else {
		f.WriteString("error: nil\n")
	}
	f.WriteString("log:\n\n")
	f.Write(out)

	f.Sync()
}

func decJobsRemain(jobQeue *JobQeue) {
	jobQeue.mux.Lock()

	jobQeue.jobsRemaining -= 1

	jobQeue.mux.Unlock()
}

func incJobsFailed(jobQeue *JobQeue) {
	jobQeue.mux.Lock()

	jobQeue.jobsFailed += 1

	jobQeue.mux.Unlock()
}

func appendJobQeueSafe(jobQeue *JobQeue, job *Job) {
	jobQeue.mux.Lock()

	jobQeue.jobs = append(jobQeue.jobs, job)

	jobQeue.jobsRemaining += 1
	jobQeue.totalJobs += 1

	jobQeue.mux.Unlock()
}

func popJobFromQeue(jobQeue *JobQeue) *Job {
	var job *Job

	jobQeue.mux.Lock()

	if len(jobQeue.jobs) > 0 {
		job = jobQeue.jobs[0]

		jobQeue.jobs = jobQeue.jobs[1:]

	} else {
		job = nil
	}
	jobQeue.mux.Unlock()

	return job
}

func jobWorker(jobWorkerCh <-chan *Job, jobColCh chan<- *Job, w *Worker, ctx *Context) {
	defer w.wg.Done()
	defer log.Println("[ ] Stop:", w.ID)

	log.Println("[ ] Start:", w.ID)

	w.wgInit.Done()

	for {
		select {
		case newJob := <-jobWorkerCh:
			if ctx.force == true {
				newJob.force = true
			}

			if newJob.force == false && jobCached(ctx, newJob) {
				newJob.state = JOB_CACHED
			} else {
				log.Println("[ ] Job started:", newJob.name, w.ID)

				newJob.started = time.Now()

				cmd := exec.Command("bash", "-c", newJob.cmd)
				cmd.Dir = ctx.outDir
				out, err := cmd.CombinedOutput()

				newJob.err = err
				newJob.stopped = time.Now()

				cacheJob(ctx, newJob, out)

				newJob.state = JOB_DONE
			}
			jobColCh <- newJob

		case <-w.stopCh:

			return
		}
	}
}

func jobInfoMsg(job *Job) string {
	var msg string

	if job.state == JOB_DONE {
		if job.err == nil {
			msg = "[+] Job success"
		} else {
			msg = "[!] Job failed"
		}
	} else if job.state == JOB_CACHED {
		msg = "[-] Job skipped"
	} else {
		msg = "[*] Job not started"
	}

	return msg
}

func jobCollector(jobColCh <-chan *Job, jobSchedCh chan<- bool, c *Collector, sc *Scheduler, jobQeue *JobQeue) {
	defer c.wg.Done()
	defer log.Println("[ ] Stop:", c.ID)

	log.Println("[ ] Start:", c.ID)

	c.wgInit.Done()

	for {
		select {
		case jobDone := <-jobColCh:
			msg := jobInfoMsg(jobDone)

			log.Printf("%s: %s %s err: %v\n", msg, jobDone.name, c.ID, jobDone.err)

			decJobsRemain(jobQeue)

			if jobDone.err == nil {
				for _, job := range jobDone.childs {
					if jobDone.state == JOB_DONE {
						job.force = true
					}
					appendJobQeueSafe(jobQeue, job)
				}
			} else {
				incJobsFailed(jobQeue)
			}
			sc.sleepMux.Lock()

			if sc.sleep == true {
				sc.sleep = false

				sc.sleepMux.Unlock()

				jobSchedCh <- true
			} else {
				sc.sleepMux.Unlock()
			}

		case <-c.stopCh:

			return
		}
	}
}

func jobScheduler(jobSchedCh <-chan bool, jobWorkerCh chan<- *Job, sc *Scheduler, jobQeue *JobQeue) {
	defer sc.wg.Done()
	defer log.Println("[ ] Stop:", sc.ID)

	log.Println("[ ] Start:", sc.ID)

	sc.wgInit.Done()

	for {
		select {
		case <-jobSchedCh:

			sc.sleepMux.Lock()

			sc.sleep = false

			sc.sleepMux.Unlock()

			for {
				job := popJobFromQeue(jobQeue)

				if job != nil {
					jobWorkerCh <- job

				} else {
					sc.sleepMux.Lock()

					sc.sleep = true

					sc.sleepMux.Unlock()

					break
				}

			}

		case <-sc.stopCh:

			return
		}
		jobQeue.mux.Lock()

		if jobQeue.jobsRemaining <= 0 {
			fmt.Println("")
			log.Println("[#] Total jobs remaining:", jobQeue.jobsRemaining)
			log.Println("[#] Total jobs loaded:", jobQeue.totalJobs)
			log.Println("[#] Total jobs failed:", jobQeue.jobsFailed)
			fmt.Println("")

			jobQeue.mux.Unlock()

			return
		}
		jobQeue.mux.Unlock()
	}
}

func createJobTree(job *Job, allJobs []string, index *int, level int) {
	r, _ := regexp.Compile("^\\s*\\+.*")

	for *index < len(allJobs) {
		newJobStr := allJobs[*index]

		*index += 1

		if r.MatchString(newJobStr) {
			newJob := parseJob(newJobStr)

			if newJob.level == level {
				job.childs = append(job.childs, &newJob)
			} else if newJob.level > level {
				childsLen := len(job.childs)

				job.childs[childsLen-1].childs = append(job.childs[childsLen-1].childs, &newJob)

				createJobTree(job.childs[childsLen-1], allJobs, index, level+1)
			} else if newJob.level == -1 {
				log.Println("Invalid job, skipping:", *index-1, newJobStr)
			} else if newJob.level < level {
				*index -= 1

				return
			}
		}
	}
}

func getJobLevel(jobStr string) int {
	i := 0

	for i < len(jobStr) {
		if jobStr[i*4] == '+' {
			return i
		} else {
			i += 1
		}
	}
	return -1
}

func parseJob(jobStr string) Job {
	sepStart := strings.Index(jobStr, "+")
	sepI := strings.Index(jobStr, ":")

	jobName := strings.TrimSpace(jobStr[sepStart+1 : sepI])

	jobCmd := strings.TrimSpace(jobStr[sepI+1:])
	jobCmd = os.ExpandEnv(jobCmd)

	jobLevel := getJobLevel(jobStr)

	// fmt.Printf("Parsed job - name: [%d] %s cmd: %s\n", jobLevel, jobName, jobCmd)

	job := Job{name: jobName, cmd: jobCmd, childs: []*Job{}, state: JOB_NOT_STARTED, level: jobLevel, force: false}

	job.uniqID = jobUniqID(&job)

	return job
}

func readJobFile(filePath string) []string {
	jobs := []string{}

	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		user, err := user.Current()

		if err != nil {
			log.Fatalf("error getting user home: %s", err.Error())
		}
		homeDirectory := user.HomeDir

		filePath = filepath.Join(homeDirectory, "gow", "jobs", filePath+".gow")
	} 

	if _, err := os.Stat(filePath); err != nil {
		log.Fatal("error loading jobs file: ", err)
	}

	f, e := os.Open(filePath)

	if e != nil {
		log.Fatal(e)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		job := scanner.Text()
		jobs = append(jobs, job)
	}

	e = scanner.Err()

	if e != nil {
		log.Fatal(e)
	}
	return jobs
}

func createDirs(ctx *Context) {
	os.MkdirAll(ctx.workDir, os.ModePerm)
	os.MkdirAll(ctx.outDir, os.ModePerm)
	os.MkdirAll(ctx.logDir, os.ModePerm)
	os.MkdirAll(ctx.inDir, os.ModePerm)
}

func jobsHelp() {
	fmt.Println(`Rules:
  - jobs at same level will run parallel
  - child jobs starts only after parrent jobs

Job definifion:
  - Syntax: 
  	- # comment
  	- +JOB_NAME : JOB_CMD
  - line started with + followed by job name and CMD
  - child job is defined by 4 spaces, only spaces are accepted no tabs
  - jobs accepts environmet variables
  - gow preset ENV variables are:
		${GOW_CMD_OPTIONS}
		${GOW_IN_F_BASEN_WO_SUFFIX}
		${GOW_IN_F}
		${GOW_IN_FN_SAFE}
		${GOW_IN_SAFE}
		${GOW_IN}
		${GOW_OUT_DIR}
		${GOW_OUT_F}
		${GOW_PROXY}
		${GOW_REPLAY_PROXY}
		${GOW_WORK_DIR}
	  	${GOW_IN_DIR}

Example:
  
+job 1: echo sleep 1s && "this will run"
+job 2: echo "this will run"
    +job 3: echo "this will run"
    +job 4: echo "this will run"
        +job 5: echo "this will run"
    #+job 6: echo "this will NOT run"
+job 7: echo "this will run"
    +job 8: echo "this will run"
`)
}

func main() {
	coreWorkersF := flag.Int("t", 5, "number of worker threads")
	workDirF := flag.String("w", "", "workspace directory for outputs, logs and results")
	inputFileF := flag.String("if", "", "input file to be processed, inputs are processed line by line")
	inputF := flag.String("i", "", "single input like url, domain, etc.")
	outFileF := flag.String("of", "", "output directory or file if needed for job processing")
	proxyF := flag.String("proxy", "", "network proxy like proto://host:port")
	replayProxyF := flag.String("replay-proxy", "", "replay proxy like proto://host:port, in case of success/finding request is sent to replay proxy e.g. Burp")
	jobsFileF := flag.String("jobs", "", "execute custom jobs file, overwrites -c")
	forceF := flag.Bool("force", false, "force job execution regardless it was run before")
	jobsExF := flag.Bool("jobs-help", false, "show gow jobs example")
	cmdOptionsF := flag.String("cmd-options", "", "put addiotnal cmd option to env variable GOW_CMD_OPTIONS")

	flag.Parse()

	if *jobsExF == true {
		jobsHelp()

		os.Exit(0)
	}

	log.Println("[ ] Start Version:", Version)

	if *workDirF == "" {
		user, err := user.Current()

		if err != nil {
			log.Fatalf("error getting user home: %s", err.Error())
		}
		homeDirectory := user.HomeDir

		*workDirF = filepath.Join(homeDirectory, "gow", "workdir")
	}

	workDirAbs, _ := filepath.Abs(*workDirF)
	inputFileAbs, _ := filepath.Abs(*inputFileF)
	outputFileAbs, _ := filepath.Abs(*outFileF)

	ctx := Context{workDir: workDirAbs, force: *forceF, inputFileSafe: fileSafeString(path.Base(inputFileAbs)), inputSafe: fileSafeString(*inputF)}

	if *inputF != "" {
		ctx.outDir = filepath.Join(workDirAbs, ctx.inputSafe, "out")
		ctx.logDir = filepath.Join(workDirAbs, ctx.inputSafe, "log")
		ctx.inDir = filepath.Join(workDirAbs, ctx.inputSafe, "in")
	} else if *inputFileF != "" {
		ctx.outDir = filepath.Join(workDirAbs, ctx.inputFileSafe, "out")
		ctx.logDir = filepath.Join(workDirAbs, ctx.inputFileSafe, "log")
		ctx.inDir = filepath.Join(workDirAbs, ctx.inputFileSafe, "in")
	} else {
		os.Exit(2)
	}

	os.Setenv("GOW_WORK_DIR", ctx.workDir)
	os.Setenv("GOW_OUT_DIR", ctx.outDir)
	os.Setenv("GOW_IN_DIR", ctx.inDir)
	os.Setenv("GOW_IN", *inputF)
	os.Setenv("GOW_IN_SAFE", ctx.inputSafe)
	os.Setenv("GOW_IN_FN_SAFE", ctx.inputFileSafe)
	os.Setenv("GOW_IN_F", inputFileAbs)
	os.Setenv("GOW_OUT_F", outputFileAbs)
	os.Setenv("GOW_IN_F_BASEN_WO_SUFFIX", strings.TrimSuffix(path.Base(inputFileAbs), path.Ext(inputFileAbs)))
	os.Setenv("GOW_PROXY", *proxyF)
	os.Setenv("GOW_REPLAY_PROXY", *replayProxyF)
	os.Setenv("GOW_CMD_OPTIONS", *cmdOptionsF)

	createDirs(&ctx)

	jobsStr := readJobFile(*jobsFileF)

	var wg sync.WaitGroup
	var wgInit sync.WaitGroup
	var wgSch sync.WaitGroup

	var workers []Worker
	var collectors []Collector

	jobQeue := JobQeue{jobs: []*Job{}, jobsRemaining: 0}

	numWorkers := *coreWorkersF

	jobWorkerCh := make(chan *Job, numWorkers)
	jobSchedCh := make(chan bool, numWorkers)
	jobColCh := make(chan *Job, numWorkers)

	sc := Scheduler{ID: "schdlr-0", stopCh: make(chan bool), wg: &wgSch, sleep: true, wgInit: &wgInit}

	wgSch.Add(1)
	wgInit.Add(1)

	go jobScheduler(jobSchedCh, jobWorkerCh, &sc, &jobQeue)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		wgInit.Add(1)

		w := Worker{ID: "worker-" + strconv.Itoa(i), stopCh: make(chan bool), wg: &wg, wgInit: &wgInit}
		workers = append(workers, w)

		go jobWorker(jobWorkerCh, jobColCh, &w, &ctx)

		wg.Add(1)
		wgInit.Add(1)
		c := Collector{ID: "collec-" + strconv.Itoa(i), stopCh: make(chan bool), wg: &wg, wgInit: &wgInit}
		collectors = append(collectors, c)

		go jobCollector(jobColCh, jobSchedCh, &c, &sc, &jobQeue)
	}

	initJob := Job{name: "init", cmd: "date; env | grep GOW_", childs: []*Job{}, state: JOB_NOT_STARTED, force: false}
	initJob.uniqID = jobUniqID(&initJob)

	index := 0

	createJobTree(&initJob, jobsStr, &index, 0)
	appendJobQeueSafe(&jobQeue, &initJob)

	wgInit.Wait()
	log.Println("[ ]   Working directory: ", ctx.workDir)
	log.Println("[ ]    Output directory: ", ctx.outDir)
	log.Println("[ ] Log files directory: ", ctx.logDir)
	log.Println("[ ] Init done")

	jobSchedCh <- true

	wgSch.Wait()

	for _, c := range collectors {
		c.stopCh <- true
		close(c.stopCh)
	}

	for _, w := range workers {
		w.stopCh <- true
		close(w.stopCh)
	}

	close(jobSchedCh)
	close(jobWorkerCh)
	close(jobColCh)

	wg.Wait()

	log.Println("[ ] Exit")
}
