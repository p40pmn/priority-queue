# Priority Queue System using Redis Sorted Sets

This project implements a **priority queue** using Redis' sorted sets to manage tasks or jobs. Redis' sorted sets provide an efficient way to manage ordered data with a unique score for each item, making them ideal for building a priority queue system.

## Features

- **Priority-based queueing**: Tasks are prioritized based on a score (e.g., timestamp, priority level).
- **Efficient**: Leveraging Redis' sorted set to ensure fast insertion and retrieval.
- **Easy integration**: Simple to integrate with your existing applications or services.

## How it works

- Each task in the queue is added to a Redis sorted set with a score that represents its priority.
- The task with the lowest score is processed first (i.e., highest priority).
- You can adjust priorities by updating the scores or adding/removing tasks.

## Installation

To get started, make sure you have Redis installed and running on your machine. Then, clone this repository and install the dependencies.

```bash
git clone https://github.com/yourusername/priority-queue.git
cd priority-queue
```
