#!/bin/sh

if [ -z "$TIBCO_HOME" ]; then
  export TIBCO_HOME /Users/larry/tibco
fi

export KAAZING_HOME=${TIBCO_HOME}/webmsgems/4.0
export KAAZING_LIBDIR=${KAAZING_HOME}/lib
if [ -f ${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.client.jar ]; then
  export KAAZING_LIBS=${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.client.jar
fi
if [ -f ${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.jms.client.jar ]; then
  export KAAZING_LIBS=${KAAZING_LIBS}:${KAAZING_LIBDIR}/client/java/com.kaazing.gateway.jms.client.jar
fi
if [ -f  ${KAAZING_LIBDIR}/geronimo-jms_1.1_spec-1.1.1.jar ]; then
  export KAAZING_LIBS=${KAAZING_LIBS}:${KAAZING_LIBDIR}/geronimo-jms_1.1_spec-1.1.1.jar
fi

echo $KAAZING_LIBS
