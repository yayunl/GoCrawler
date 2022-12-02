package main

import (
	"GoCrawler/pattern"
	"context"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"net/http"
	"time"
)

type Author struct {
	Name        string
	BornDate    string
	BornPlace   string
	Description string
	AboutURI    string
}

func NewAuthor() *Author {
	return &Author{
		Name:        "",
		BornDate:    "",
		BornPlace:   "",
		Description: "",
		AboutURI:    "",
	}
}

var crawlerFnc = func(urls []string) (pattern.Result, []string) {
	// This is the execution function that is used to parse a page and
	// returns the result through `Result`.
	url := urls[0]
	resp, err := http.Get(url)
	if err != nil {
		return pattern.Result{Err: err, Value: nil}, nil
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		author := NewAuthor()
		author.AboutURI = url
		return pattern.Result{Err: err, Value: author}, nil
	}
	listUrls, err := pattern.Extract(url)

	// Extract data from the response content
	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		author := NewAuthor()
		author.AboutURI = url
		return pattern.Result{Err: err, Value: author}, nil
	}

	// Find the review items
	author := NewAuthor()
	author.AboutURI = url
	doc.Find(".author-details").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the title
		name := s.Find(".author-title").Text()
		bornD := s.Find(".author-born-date").Text()
		bornP := s.Find(".author-born-location").Text()
		des := s.Find(".author-description").Text()

		author.Name = name
		author.BornDate = bornD
		author.BornPlace = bornP
		author.Description = des
	})
	//fmt.Printf("Author=> %v\n", author)
	res := pattern.Result{Value: author}
	return res, listUrls
}

func createTasks(urls []string) []pattern.Job {
	var tasks []pattern.Job

	for _, url := range urls {
		newTask := pattern.Job{
			ID:   url,
			Fn:   &crawlerFnc,
			Args: []string{url},
		}
		tasks = append(tasks, newTask)
	}
	return tasks
}

func main() {
	start := time.Now()

	// Create channels for global control
	done := make(chan struct{})
	jobStream := make(chan *pattern.Job)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	defer close(done)
	defer close(jobStream)

	// Step 1: Create a batch of tasks
	tasks := createTasks([]string{"http://quotes.toscrape.com/"})
	// Step 2: Create a pool of workers
	wp := pattern.New(20, len(tasks))

	// Step 3: Assign the tasks to the workers
	pattern.JobGenerator(done, jobStream, tasks)
	wp.Assign(jobStream)
	// Step 4: Let the workers run the assigned tasks
	go wp.Run(ctx)
	// Step 5: Gather the results
	results := wp.Results()
	// Step 6: Only get the valid results, meaning response has no error and contains valid authors
	authorStream := func(inDataStream <-chan pattern.Result) <-chan *Author {
		resultStream := make(chan *Author)
		go func() {
			defer close(resultStream)
			for r := range inDataStream {
				if r.Err == nil && r.Value.(*Author).Name != "" { // author name is not empty
					resultStream <- r.Value.(*Author)
				}
				// Whenever the jobs in the queue become empty, stop all goroutines/workers.
				wp.JobInQueue -= 1
				if wp.JobInQueue == 0 {
					break
				}
			}
		}()
		return resultStream
	}

	discoveredAuthors := 0
	for author := range authorStream(results) {
		fmt.Printf("name: %s, born date: %s, born place: %s, about: %s \n", author.Name, author.BornDate, author.BornPlace, author.AboutURI)
		discoveredAuthors += 1
	}
	fmt.Printf("Total items scraped: %d\n", discoveredAuthors)
	fmt.Printf("Total elapsed time: %v\n", time.Since(start))
}
