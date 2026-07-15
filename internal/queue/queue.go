package queue

import "github.com/VasuBhakt/vahak/internal/models"

type JobQueue struct {
	Jobs chan models.DeliveryJob
}

func NewJobQueue(size int) *JobQueue {
	return &JobQueue{
		Jobs: make(chan models.DeliveryJob, size),
	}
}

// Push adds a job to the queue. If the queue is full, it drops the push
// so it doesn"t block the HTTP request. The DB Sweeper will pick up the dropped job.
func (q *JobQueue) Push(job models.DeliveryJob) {
	select {
	case q.Jobs <- job:
	default:
		// Queue is full. Drop the message.
	}
}

