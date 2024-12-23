package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrQueueEmpty = fmt.Errorf("queue is empty")

const (
	// queueKey is the key used to store the queue in Redis.
	queueKey = "queue:%s"

	// dequeueKey is the key used to store the dequeued items in Redis.
	dequeueKey = "dequeue:%s"

	// releaseKey is the key used to store the released items in Redis.
	releaseKey = "release:%s"

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

	err := redisClient.
		FTCreate(
			ctx,
			fmt.Sprintf(idxKey, "dequeue"),
			&redis.FTCreateOptions{
				OnJSON: true,
				Prefix: []interface{}{"dequeue:"},
			},
			&redis.FieldSchema{
				FieldName: "$.id",
				As:        "id",
				FieldType: redis.SearchFieldTypeTag,
			},
			&redis.FieldSchema{
				FieldName: "$.members",
				As:        "members",
				FieldType: redis.SearchFieldTypeTag,
			},
		).
		Err()
	if err != nil && err.Error() != "Index already exists" {
		return nil, fmt.Errorf("failed to create index: %w", err)
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

	// FirstDequeue indicates whether to dequeue the first N items (true) or a single item (false).
	FirstDequeue bool

	// FirstDequeueNumber is the number of items to dequeue if FirstDequeue is true.
	// If 0, a single item is dequeued by default.
	FirstDequeueNumber int

	// ReleaseAll indicates whether to release all items in the queue.
	// If true, all items in the queue are dequeued regardless of other fields.
	ReleaseAll bool
}

// Dequeue removes one or more items from the specified queue.
//
// The function behavior is controlled by the fields in the DequeueReq struct:
//   - If ReleaseAll is true, all items in the queue are removed.
//   - If FirstDequeue is true, the first N items are removed, where N is the value of FirstDequeueNumber.
//     If FirstDequeueNumber is 0, a single item is removed by default.
//   - If FirstDequeue is false, a single item is removed from the queue.
//
// Returns:
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

	if in.ReleaseAll {
		return dequeueAllMembersByScore(ctx, q.redisClient, in.ID)
	}

	if in.FirstDequeue && in.FirstDequeueNumber > 1 {
		return dequeueByRank(ctx, q.redisClient, in.ID, int64(in.FirstDequeueNumber-1))
	}

	return dequeueByRank(ctx, q.redisClient, in.ID, 0)
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
	isRelease, err := q.redisClient.
		Exists(
			ctx,
			fmt.Sprintf(releaseKey, queueID),
		).
		Result()
	if err == nil && isRelease == 1 {
		return true, nil
	}

	members, err := q.redisClient.
		FTSearchWithArgs(
			ctx,
			fmt.Sprintf(idxKey, "dequeue"),
			fmt.Sprintf("@id:{%s} @members:{%s}", queueID, memberID),
			&redis.FTSearchOptions{
				WithScores: true,
				Limit:      1,
			},
		).Result()
	if err != nil {
		return false, err
	}

	return members.Total > 0, nil
}

type dequeueKyToStore struct {
	ID      string   `json:"id"`
	Members []string `json:"members"`
}

func dequeueAllMembersByScore(ctx context.Context, redisClient *redis.Client, queueID string) ([]string, error) {
	members, err := redisClient.
		ZRange(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			0,
			-1,
		).
		Result()
	if err != nil {
		return []string{}, err
	}

	err = redisClient.
		ZRemRangeByScore(
			ctx,
			fmt.Sprintf(queueKey, queueID),
			"-inf", "+inf",
		).
		Err()
	if err != nil {
		return []string{}, err
	}

	if err := redisClient.Set(
		ctx,
		fmt.Sprintf(releaseKey, queueID),
		true,
		0,
	).
		Err(); err != nil {
		return []string{}, err
	}

	return members, nil
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

	var d dequeueKyToStore
	d.ID = queueID
	d.Members = members
	uid := uuid.New().String()

	err = redisClient.JSONSet(
		ctx,
		fmt.Sprintf(dequeueKey, strings.Split(uid, "-")[4]),
		"$",
		d,
	).
		Err()
	if err != nil {
		return []string{}, err
	}

	return members, nil
}
