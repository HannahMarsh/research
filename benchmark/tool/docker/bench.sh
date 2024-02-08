#!/bin/bash

TYPE=$1

# Direcotry to save logs
LOG=./logs

RECORDCOUNT=100000
OPERATIONCOUNT=100000
THREADCOUNT=20
FIELDCOUNT=5
FIELDLENGTH=16
MAXSCANLENGTH=10

PROPS="-p recordcount=${RECORDCOUNT} \
    -p operationcount=${OPERATIONCOUNT} \
    -p threadcount=${THREADCOUNT} \
    -p fieldcount=${FIELDCOUNT} \
    -p fieldlength=${FIELDLENGTH} \
    -p maxscanlength=${MAXSCANLENGTH}"
PROPS+=" ${*:3}"
WORKLOADS=

mkdir -p ${LOG} 

BENCH_DB=cassandra

PROPS+=" -p cassandra.cluster=cassandra"
SLEEP_TIME=30

echo ${TYPE} ${WORKLOADS} ${PROPS}

CMD="docker-compose -f ${BENCH_DB}.yml" 

if [ ${TYPE} == 'load' ]; then 
    $CMD down --remove-orphans
    rm -rf ./data/${BENCH_DB}
    $CMD up -d
    sleep ${SLEEP_TIME}

    $CMD run ycsb load cassandra ${WORKLOADS} -p workload=core ${PROPS} | tee ${LOG}/${BENCH_DB}_load.log

    $CMD down
elif [ ${TYPE} == 'run' ]; then
    $CMD up -d
    sleep ${SLEEP_TIME}

    for workload in a b c d e f 
    do 
        $CMD run --rm ycsb run cassandra -P ../../workloads/workload${workload} ${WORKLOADS} ${PROPS} | tee ${LOG}/${BENCH_DB}_workload${workload}.log
    done

    $CMD down
else
    echo "invalid type ${TYPE}"
    exit 1
fi 
