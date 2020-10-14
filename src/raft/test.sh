#!/bin/bash

rm log -rf

num_tests=10

function mytest() {
    task="$1"
    logfile="log/$task"

    mkdir log
    rm $logfile -f

    for (( i=0; i<num_tests; i++ )); do
        echo -e "\nstart test $i \n" >> $logfile
        go test -run $task >> $logfile
    done
}
    
mytest 2B
