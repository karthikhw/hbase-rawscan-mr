# hbase-rawscan-mr

We do not have any MR for Hbase table raw scan mode. This can be used it Instead.

java -cp \`hbase classpath\`:hbase-rawscan-mr-1.2-SNAPSHOT.jar com.hbase.util.RawScanDriver [--zookpeer-server \<comma seperated list\>] [--zookeper-znode \<znode\>] [--zookeeper-port \<zk port\>] --table-name \<tablename\>  --output-path \<path\>

NOTE: Download hbase-rawscan-mr-1.2-SNAPSHOT.jar from /target.
