#!/bin/sh

export TARGETCLASS=com.tibco.mcqueary.jmsperf.jmsMsgProducerPerf
export FLAVOR=tibems
export PROPERTIES=provider/$FLAVOR/${FLAVOR}.properties

export WMQ_HOME=/opt/mqm
export WMQ_CLASSPATH="${WMQ_HOME}/java/lib/*"

export TIBEMS_HOME=/opt/tibco/ems/6.3
export TIBEMS_CLASSPATH="${TIBEMS_HOME}/lib/*"


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
  export CLASSPATH=jmsperf.jar:${TIBEMS_CLASSPATH}
else
  export CLASSPATH=jmsperf.jar:${WMQ_CLASSPATH}
fi

java -cp $CLASSPATH $TARGETCLASS -propFile $PROPERTIES
