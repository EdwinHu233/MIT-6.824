# MIT-6.824

This is my solution to MIT 6.824 labs (spring 2020). All materials are in [the MIT website](http://nil.csail.mit.edu/6.824/2020/schedule.html).

Working in progress.

## Finished Labs

### Map Reduce

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

### Raft

#### 2-A. Leader Election

Source codes are in `src/raft`

To run the test:

```bash
cd src/main
go test -run 2A
```
