#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# 
# Runs a Hadoop hbase command as a daemon.
#
# Environment Variables
#
#   HAMA_CONF_DIR   Alternate hbase conf dir. Default is ${HAMA_HOME}/conf.
#   HAMA_LOG_DIR    Where log files are stored.  PWD by default.
#   HAMA_PID_DIR    The pid files are stored. /tmp by default.
#   HAMA_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   HAMA_NICENESS The scheduling priority for daemons. Defaults to 0.
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh

usage="Usage: hama-daemon.sh [--config <conf-dir>]\
 (start|stop) <hama-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/hama-config.sh

# get arguments
startStop=$1
shift

command=$1
shift

hama_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
        num=$prev
    done
    mv "$log" "$log.$num";
    fi
}

if [ -f "${HAMA_CONF_DIR}/hama-env.sh" ]; then
  . "${HAMA_CONF_DIR}/hama-env.sh"
fi

# get log directory
if [ "$HAMA_LOG_DIR" = "" ]; then
  export HAMA_LOG_DIR="$HAMA_HOME/logs"
fi
mkdir -p "$HAMA_LOG_DIR"

if [ "$HAMA_PID_DIR" = "" ]; then
  HAMA_PID_DIR=/tmp
fi

if [ "$HAMA_IDENT_STRING" = "" ]; then
  export HAMA_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java
export HAMA_LOGFILE=hama-$HAMA_IDENT_STRING-$command-$HOSTNAME.log
export HAMA_ROOT_LOGGER="INFO,DRFA"
logout=$HAMA_LOG_DIR/hama-$HAMA_IDENT_STRING-$command-$HOSTNAME.out  
loglog="${HAMA_LOG_DIR}/${HAMA_LOGFILE}"
pid=$HAMA_PID_DIR/hama-$HAMA_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$HAMA_NICENESS" = "" ]; then
    export HAMA_NICENESS=0
fi

case $startStop in

  (start)
    mkdir -p "$HAMA_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    hama_rotate_log $logout
    echo starting $command, logging to $logout
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    echo "ulimit -n `ulimit -n`" >> $loglog 2>&1
    nohup nice -n $HAMA_NICENESS "$HAMA_HOME"/bin/hama \
        --config "${HAMA_CONF_DIR}" \
        $command $startStop "$@" > "$logout" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$logout"
    ;;

  (stop)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Stopping $command" >> $loglog
        if [ "$command" = "master" ]; then
          nohup nice -n $HAMA_NICENESS "$HAMA_HOME"/bin/hama \
              --config "${HAMA_CONF_DIR}" \
              $command $startStop "$@" > "$logout" 2>&1 < /dev/null &
        else
          echo "`date` Killing $command" >> $loglog
          kill `cat $pid` > /dev/null 2>&1
        fi
        while kill -0 `cat $pid` > /dev/null 2>&1; do
          echo -n "."
          sleep 1;
        done
        echo
      else
        retval=$?
        echo no $command to stop because kill of pid `cat $pid` failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
