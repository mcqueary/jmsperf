#!/bin/sh

export JVM_OPTS="-Xms1024m -Xmx2048m"

JAR=target/jmsperf-0.0.1-SNAPSHOT.jar

export TIBCO_HOME=/Users/larry/tibco

export TARGETCLASS=com.tibco.mcqueary.jmsperf.jmsMsgConsumerPerf
export FLAVOR=tibems
export PROPERTIES=provider/$FLAVOR/${FLAVOR}.properties

export KAAZING_HOME=${TIBCO_HOME}/webmsgems/4.0
export KAAZING_LIBDIR=${KAAZING_HOME}/lib
if [ -f ${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.client.jar ]; then
  export KAAZING_JARS=${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.client.jar
fi
if [ -f ${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.jms.client.jar ]; then
  export KAAZING_JARS=${KAAZING_JARS}:${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.jms.client.jar
fi
if [ -f  ${KAAZING_LIBDIR}/geronimo-jms_1.1_spec-1.1.1.jar ]; then
  export KAAZING_JARS=${KAAZING_JARS}:${KAAZING_LIBDIR}/geronimo-jms_1.1_spec-1.1.1.jar
fi

if [ -z "$TIBEMS_ROOT" ]; then
  export TIBEMS_HOME=/Users/larry/tibco/ems/6.3
fi
if [ -f $TIBEMS_ROOT/lib/tibjms.jar ]; then
  export TIBEMS_JARS=`echo ${TIBEMS_ROOT}/lib/*.jar | tr " " ":"`
fi


usage ()
{
  echo "Usage: $0 <tibems|kaazing> <properties file>"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
else 
  export FLAVOR=$1
  export PROPERTIES=$2
fi

if [ $FLAVOR != "tibems" -a $FLAVOR != "kaazing" ]; then
  echo "Must choose either tibems or kaazing"
  usage
elif [ ! -r $PROPERTIES ]; then
  echo "Cannot read properties file $PROPERTIES"
  usage
fi

if [ $FLAVOR = "tibems" ]; then
  export CLASSPATH=${JAR}:${TIBEMS_JARS}:provider/tibems/jndi.properties
else
  export CLASSPATH=${JAR}:${KAAZING_JARS}:provider/kaazing/jndi.properties
fi

CMD="java $JVM_OPTS -cp $CLASSPATH $TARGETCLASS -propFile $PROPERTIES"
$CMD
