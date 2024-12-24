package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var ErrQueueEmpty = fmt.Errorf("queue is empty")

const (
	// queueKey is the key used to store the queue in Redis.
	queueKey = "queue:%s"

	// dequeueKey is the key used to store the dequeued items in Redis.
	dequeueKey = "dequeue:%s"

	// clearKey is the key used to store the clear flag in Redis.
	clearKey = "clear:%s"

	// idxKey is the key used to store the index in Redis.
	idxKey = "idx:%s"
)

// Service represents a service for enqueueing and dequeueing items from a Redis instance.
type Service struct {
	redisClient *redis.Client
}

// NewService returns a new Service for enqueueing and dequeueing items from a Redis instance.
//
// The context.Context is not used in this function and is only present for forward
// compatibility.
func NewService(ctx context.Context, redisClient *redis.Client) (*Service, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	return &Service{
		redisClient: redisClient,
	}, nil
}

// EnqueueReq represents a request to enqueue an item into a queue.
type EnqueueReq struct {
	// The unique identifier for the queue.
	ID string

	// The unique identifier of the item being enqueued.
	MemberID string

	// Priority score (lower is higher priority)
	Score float64
}

// Enqueue adds an item to the Redis queue with a specified priority score.
//
// The item's priority is determined by the score value, where lower scores
// indicate higher priority. The function uses the Redis ZAdd command to
// add the item to a sorted set corresponding to the queue specified by in.ID.
//
//   - ctx: The context for the request, used for cancellation and timeouts.
//   - in: A pointer to an EnqueueReq containing the queue ID, the item ID (MemberID),
//     and the priority score.
//
// Returns:
//   - An error if the operation fails; otherwise, nil.
func (q *Service) Enqueue(ctx context.Context, in *EnqueueReq) error {
	return q.redisClient.
		ZAdd(
			ctx,
			fmt.Sprintf(queueKey, in.ID),
			redis.Z{
				Score:  in.Score,
				Member: in.MemberID,
			}).
		Err()
}

// DequeueReq represents a request to dequeue an item from a queue.
type DequeueReq struct {
	// The unique identifier for the queue.
	ID string

	// Number is the number of items to dequeue.
	// If 0, a single item is dequeued by default.
	Number int
}

// Dequeue removes one or more items from the specified queue.
//
// The function retrieves and removes the specified number of items from the queue,
// starting from the item with the highest priority (lowest score). If the "Number"
// field in the request is greater than 1, it removes multiple items up to the specified
// number. If it is 0 or not specified, a single item is removed by default.
//
// Returns:
//   - A slice of strings containing the dequeued item IDs.
//   - An error if the operation fails; otherwise, nil.
func (q *Service) Dequeue(ctx context.Context, in *DequeueReq) ([]string, error) {
	queueLen, err := q.redisClient.
		ZCard(
			ctx,
			fmt.Sprintf(queueKey, in.ID),
		).
		Uint64()
	if err != nil {
		return []string{}, err
	}
	if queueLen == 0 {
		return []string{}, nil
	}

	if in.Number > 1 {
		return dequeueByRank(ctx, q.redisClient, in.ID, int64(in.Number-1))
	}

	return dequeueByRank(ctx, q.redisClient, in.ID, 0)
}

func (q *Service) Clear(ctx context.Context, queueID string) error {
	queueLen, err := q.redisClient.
		ZCard(
			ctx,
			fmt.Sprintf(queueKey, queueID),
		).
		Uint64()
	if err != nil {
		return err
	}
	if queueLen == 0 {
		return nil
	}

	err = q.redisClient.
		ZRemRangeByScore(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			"-inf", "+inf",
		).
		Err()
	if err != nil {
		return err
	}

	if err := q.redisClient.Set(
		ctx,
		fmt.Sprintf(clearKey, queueID),
		true,
		0,
	).
		Err(); err != nil {
		return err
	}
	return nil
}

// PeekByQueueID returns the first item in the specified queue.
//
// The function returns an empty string and an error if the queue is empty.
//
// Returns:
//   - The first item in the queue, or an empty string if the queue is empty.
//   - An error if the operation fails; otherwise, nil.
func (q *Service) PeekByQueueID(ctx context.Context, queueID string) (string, error) {
	members, err := q.redisClient.
		ZRange(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			0,
			0,
		).
		Result()
	if err != nil {
		return "", err
	}
	if len(members) == 0 {
		return "", ErrQueueEmpty
	}
	return members[0], nil
}

// PositionReq represents a request to get the position of an item in a queue.
type PositionReq struct {
	// The unique identifier for the queue.
	ID string

	// The member ID of the item to get the position of.
	MemberID string
}

// GetPosition returns the position of an item in a queue, with the first item being 0.
//
// The function returns an error if the queue is empty.
func (q *Service) GetPosition(ctx context.Context, in *PositionReq) (uint64, error) {
	count, err := q.redisClient.ZCard(
		ctx,
		fmt.Sprintf(queueKey, in.ID),
	).
		Uint64()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, ErrQueueEmpty
	}

	return q.redisClient.
		ZRank(ctx,
			fmt.Sprintf(queueKey, in.ID),
			in.MemberID,
		).
		Uint64()
}

// SetPriorityReq represents a request to set or update the priority score of an item in a queue.
type SetPriorityReq struct {
	// ID is the unique identifier for the queue to which the item belongs.
	ID string

	// MemberID is the unique identifier of the queue item whose priority is being updated.
	MemberID string

	// Score is the new priority score for the queue item.
	// Lower scores indicate higher priority.
	Score float64
}

// SetPriority sets or updates the priority score of an item in a queue.
//
// The function behavior is as follows:
//   - If the item does not exist in the queue, it is added with the given score.
//   - If the item already exists in the queue, its score is updated.
//
// Returns:
//   - An error if the operation fails; otherwise, nil.
func (q *Service) SetPriority(ctx context.Context, in *SetPriorityReq) error {
	return q.redisClient.ZAdd(
		ctx,
		fmt.Sprintf(queueKey, in.ID),
		redis.Z{
			Score:  in.Score,
			Member: in.MemberID,
		},
	).Err()
}

// DeleteReq represents a request to delete an item from a queue.
type DeleteReq struct {
	// The unique identifier for the queue.
	ID string

	// The member ID of the item to delete.
	MemberID string
}

// Delete removes an item from the specified queue.
//
// Returns:
//   - An error if the operation fails; otherwise, nil.
func (q *Service) Delete(ctx context.Context, in *DeleteReq) error {
	return q.redisClient.
		ZRem(
			ctx,
			fmt.Sprintf(queueKey, in.ID),
			in.MemberID,
		).Err()
}

// IsDequeued returns true if the specified item has been dequeued from the queue.
//
// The function checks for the existence of the item in the "dequeue" index
// and if the item is in the "release" set.
//
// The function returns an error if the operation fails; otherwise, nil.
func (q *Service) IsDequeued(ctx context.Context, queueID string, memberID string) (bool, error) {
	isCleared, err := q.redisClient.
		Exists(
			ctx,
			fmt.Sprintf(clearKey, queueID),
		).
		Result()
	if err == nil && isCleared == 1 {
		return true, nil
	}

	isDequeued, err := q.redisClient.
		SIsMember(
			ctx,
			fmt.Sprintf(dequeueKey, queueID),
			memberID,
		).
		Result()
	if err != nil {
		return false, err
	}

	return isDequeued, nil
}

func dequeueByRank(ctx context.Context, redisClient *redis.Client, queueID string, stop int64) ([]string, error) {
	members, err := redisClient.
		ZRange(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			0,
			stop,
		).
		Result()
	if err != nil {
		return []string{}, err
	}

	err = redisClient.
		ZRemRangeByRank(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			0,
			stop,
		).
		Err()
	if err != nil {
		return []string{}, err
	}

	if err := redisClient.SAdd(
		ctx,
		fmt.Sprintf(dequeueKey, queueID),
		members,
	).Err(); err != nil {
		return []string{}, err
	}

	return members, nil
}
