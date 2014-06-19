#!/bin/sh

export TARGETCLASS=com.tibco.mcqueary.jmsperf.jmsMsgProducerPerf
export FLAVOR=tibems
export PROPERTIES=provider/$FLAVOR/${FLAVOR}.properties

export WMQ_HOME=/opt/mqm
export WMQ_CLASSPATH="${WMQ_HOME}/java/lib/*"

if [ -z "$TIBEMS_ROOT" ]; then
  export TIBEMS_ROOT=/Users/larry/tibco/ems/7.0
fi
export TIBEMS_CLASSPATH=`echo $TIBEMS_ROOT/lib/*.jar | tr " " ":"`


usage ()
{
  echo "Usage: $0 <tibems|wmq> <properties file>"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
else 
  export FLAVOR=$1
  export PROPERTIES=$2
fi

if [ $FLAVOR != "tibems" -a $FLAVOR != "wmq" ]; then
  echo "Must choose either tibems or wmq"
  usage
elif [ ! -r $PROPERTIES ]; then
  echo "Cannot read properties file $PROPERTIES"
  usage
fi

if [ $FLAVOR = "tibems" ]; then
  export CLASSPATH=target/classes:${TIBEMS_CLASSPATH}
else
  export CLASSPATH=target/classes:${WMQ_CLASSPATH}
fi

java -cp $CLASSPATH $TARGETCLASS -propFile $PROPERTIES
