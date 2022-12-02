package pattern

import (
	"context"
	"fmt"
	"strings"

	//"fmt"
	"sync"
)

type Result struct {
	JobID       interface{}
	Value       interface{}
	Err         error
	Description string
}

// Tasks

type Tasker interface {
	Execute() (Result, []string)
}

// Job implements `Tasker` interface

type Job struct {
	ID   any
	Fn   *func(urls []string) (Result, []string)
	Args []string
}

func NewJob(id string, jobExecFnc *func(urls []string) (Result, []string), args []string) *Job {
	return &Job{
		ID:   id,
		Fn:   jobExecFnc,
		Args: args,
	}
}

func (t Job) Execute() (Result, []string) {
	result, addJobs := (*t.Fn)(t.Args)
	return result, addJobs
}

func ExecuteTask(t Tasker) (Result, []string) {
	return t.Execute()
}

// WorkerPool

type WorkerPool struct {
	MaxWorkers   int
	JobStream    chan *Job
	JobInQueue   int
	JobSeenList  map[string]bool
	ResultStream chan Result
	Done         chan struct{}
}

func New(maxWorkers int, initialJobs int) *WorkerPool {
	return &WorkerPool{
		MaxWorkers:   maxWorkers,
		JobStream:    make(chan *Job, 255),
		JobInQueue:   initialJobs,
		JobSeenList:  map[string]bool{},
		ResultStream: make(chan Result, 255),
		Done:         make(chan struct{}),
	}
}

func (wp *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// Create the number of `wp.MaxWorkers` workers
	for i := 0; i < wp.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int, ctx context.Context, wg *sync.WaitGroup, jobStream chan *Job, resultStream chan<- Result) {
			defer wg.Done()
			for {
				select {
				// Fetch one job from the jobStream channel
				case task, ok := <-jobStream:
					if !ok {
						// Jobs channel is closed
						return
					}
					// execute the job and gather results
					res, addJobIDs := ExecuteTask(task)
					wp.ResultStream <- res

					if len(addJobIDs) != 0 {
						for _, newURI := range addJobIDs {
							if !wp.JobSeenList[newURI] && (strings.Contains(newURI, "author") || strings.Contains(newURI, "page")) {
								// Only add the URIs about the author
								wp.JobSeenList[newURI] = true
								// Add additional jobs to the job queue
								//fmt.Printf("Adding the new task: %s\n", newURI)
								newJob := NewJob(newURI, task.Fn, []string{newURI})

								wp.JobInQueue += 1
								wp.JobStream <- newJob
							}
						}
					}

				case <-ctx.Done():
					value := fmt.Sprintf("cancelled worker %d. Error detail: %v\n", workerID, ctx.Err())
					resultStream <- Result{
						Value: value,
						Err:   ctx.Err(),
					}
					return
				}
			}
		}(i, ctx, &wg, wp.JobStream, wp.ResultStream)
	}

	// Fan-in results
	wg.Wait()
	close(wp.ResultStream)
}

func (wp *WorkerPool) Results() <-chan Result {
	return wp.ResultStream
}

func (wp *WorkerPool) Assign(jobs chan *Job) {
	wp.JobStream = jobs
}

func JobGenerator(done <-chan struct{}, jobStream chan *Job, tasks []Job) {
	// Input arg `tasks` is a slice of tasks
	//jobStream := make(chan Job)

	// Use `seen` map to delete duplicate jobs
	seen := make(map[any]bool)
	// a pipeline stage for job creation
	go func() {
		for _, task := range tasks {
			if !seen[task.ID] {
				select {
				case <-done:
					return
				case jobStream <- &task:
				}
			}
		}
	}()
	//return jobStream
}
