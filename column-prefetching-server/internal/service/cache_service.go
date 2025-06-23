package service

import (
	projectconfig "column-prefetching-server/internal/project-config"
	"context"
	"fmt"
	"github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"github.com/valkey-io/valkey-glide/go/v2/options"
	"github.com/valkey-io/valkey-glide/go/v2/pipeline"
	"time"
)

func NewCacheService(cfg projectconfig.CacheConfig) (*CacheService, error) {
	// TODO: decide if we want to pass in the host and port from AAL via the HTTP request to CPS endpoint
	host := cfg.ElastiCacheEndpoint
	port := cfg.ElastiCachePort
	clusterConfig := config.NewClusterClientConfiguration().
		WithAddress(&config.NodeAddress{Host: host, Port: port}).
		WithUseTLS(true).
		WithRequestTimeout(cfg.RequestTimeout)
	client, err := glide.NewClusterClient(clusterConfig)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	service := &CacheService{
		elastiCacheClient: *client,
		config:            cfg,
		batchRequests:     make(chan SetRequest, 1000),
		ctx:               ctx,
		cancel:            cancel,
		batcherStarted:    false,
	}

	service.startBatching()

	return service, nil
}

func (service *CacheService) CacheColumnData(data parquetColumnData) {
	service.startBatching()

	cacheKey := generateCacheKey(data)

	service.batchRequests <- SetRequest{
		Key:   cacheKey,
		Value: string(data.data),
	}
}

func (service *CacheService) startBatching() {
	service.mu.Lock()
	defer service.mu.Unlock()

	// we only want to start the batch processor once, so return if already started
	if service.batcherStarted {
		return
	}

	service.batcherStarted = true
	service.wg.Add(1)
	go service.batchProcessor()
}

func (service *CacheService) batchProcessor() {
	defer service.wg.Done()

	var currentBatch *pipeline.ClusterBatch
	var setBatch []SetRequest
	timer := time.NewTimer(service.config.BatchTimeout)
	defer timer.Stop()

	// anonymous function to create a new batch of cache set operations
	resetBatch := func() {
		currentBatch = pipeline.NewClusterBatch(false)
		setBatch = make([]SetRequest, 0, service.config.BatchSize)
	}

	// anonymous function to process accumulated cache set operations
	sendBatch := func() {
		if len(setBatch) == 0 {
			fmt.Printf("No items in batch, skipping send")
			return
		}

		fmt.Printf("Sending batch of %d items to ElastiCache", len(setBatch))

		service.elastiCacheClient.Exec(service.ctx, *currentBatch, false)

		resetBatch()
	}

	// create an initial batch
	resetBatch()

	timer.Reset(service.config.BatchTimeout)

	fmt.Printf("Batch processor started")

	for {
		select {
		// received a new request, add it to the set batch
		case req, _ := <-service.batchRequests:
			fmt.Printf("Adding item to batch. Current batch size: %d/%d",
				len(setBatch)+1, service.config.BatchSize)

			// add request to set batch
			setBatch = append(setBatch, req)

			// add the set command to the batch operation
			setOptions := options.NewSetOptions().
				SetExpiry(options.NewExpiryIn(service.config.TimeToLive))
			currentBatch.SetWithOptions(req.Key, req.Value, *setOptions)

			// check if the batch is full, process it if so
			if len(setBatch) >= service.config.BatchSize {
				if !timer.Stop() {
					<-timer.C
				}
				sendBatch()
				timer.Reset(service.config.BatchTimeout)
			}

		//	the timer is done, so process the batch
		case <-timer.C:
			fmt.Printf("Batch timeout reached. Sending batch with %d items", len(setBatch))
			sendBatch()
			fmt.Printf("Timeout batch sent. Creating new batch.")
			timer.Reset(service.config.BatchTimeout)
		}
	}
}

func generateCacheKey(data parquetColumnData) string {
	s3URI := fmt.Sprintf("s3://%s/%s", data.bucket, data.key)
	return fmt.Sprintf("%s#%s#%s", s3URI, data.etag, data.columnRange)
}
