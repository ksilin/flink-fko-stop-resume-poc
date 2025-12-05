FROM flink:1.20-java11

# Copy the jar to Flink's lib directory
COPY target/flink-deployment-test-1.0-SNAPSHOT.jar /opt/flink/usrlib/

USER flink
