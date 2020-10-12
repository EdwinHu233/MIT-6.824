# MIT-6.824

This is my solution to MIT 6.824 labs (spring 2020). All materials are in [the MIT website](http://nil.csail.mit.edu/6.824/2020/schedule.html).

Working in progress.

## Finished Labs

### Lab 1. Map Reduce

Source codes are in `src/mr`.

To run the test script:

```bash
cd src/main
bash test-mr.sh 2> log
```

To clean up:

```bash
# still in src/main
rm log mr-tmp/ mrsequential mrmaster mrworker -r
```

### Lab 2. Raft

Source codes are in `src/raft`

#### 2A. Leader Election

To run the test:

```bash
cd src/raft
go test -run 2A
```

#### 2B. Log Prelication

To run the test:

```bash
cd src/raft
go test -run 2B
```

#### 2C. Persistence

To run the test:

```bash
cd src/raft
go test -run 2C
```

(This part is not perfectly done yet).
