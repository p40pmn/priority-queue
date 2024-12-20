package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/p40pmn/priority-queue/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", getEnv("REDIS_HOST", "127.0.0.1"), getEnv("REDIS_PORT", "6379")),
		MinIdleConns: 10,
		PoolSize:     15,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis client not connected: %v", err)
	}
	defer redisClient.Close()

	q, err := queue.NewService(ctx, redisClient)
	if err != nil {
		log.Fatalf("failed to create queue service: %v", err)
	}

	if err := q.Enqueue(ctx, &queue.EnqueueReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_PAO",
		Score:    1,
	}); err != nil {
		log.Fatalf("failed to enqueue item: %v", err)
	}

	if err := q.Enqueue(ctx, &queue.EnqueueReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_DAKY",
		Score:    2,
	}); err != nil {
		log.Fatalf("failed to enqueue item: %v", err)
	}

	if err := q.Enqueue(ctx, &queue.EnqueueReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_VONGXAY",
		Score:    2,
	}); err != nil {
		log.Fatalf("failed to enqueue item: %v", err)
	}

	if err := q.Enqueue(ctx, &queue.EnqueueReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_PHONGPHAT",
		Score:    2,
	}); err != nil {
		log.Fatalf("failed to enqueue item: %v", err)
	}

	if err := q.Dequeue(ctx, &queue.DequeueReq{
		ID:                 "LITD_QUEUE",
		ReleaseAll:         false,
		FirstDequeue:       true,
		FirstDequeueNumber: 1,
	}); err != nil {
		log.Fatalf("failed to dequeue item: %v", err)
	}

	member, err := q.PeekByQueueID(ctx, "LITD_QUEUE")
	if err != nil {
		log.Fatalf("failed to peek item: %v", err)
	}

	fmt.Println("peeked item: ", member)

	position, err := q.GetPosition(ctx, &queue.PositionReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_VONGXAY",
	})
	if err != nil {
		log.Fatalf("failed to get position: %v", err)
	}
	fmt.Println("position: ", position)

	if err := q.SetPriority(ctx, &queue.SetPriorityReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_VONGXAY",
		Score:    3,
	}); err != nil {
		log.Fatalf("failed to set priority: %v", err)
	}

	if err := q.Delete(ctx, &queue.DeleteReq{
		ID:       "LITD_QUEUE",
		MemberID: "LITD_MEMBER_VONGXAY",
	}); err != nil {
		log.Fatalf("failed to delete item: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}