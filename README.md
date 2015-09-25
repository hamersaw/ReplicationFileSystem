A distributed file system implementation using replication between nodes to guarantee accessibility in case of node failure.

java -cp build/libs/ReplicationFileSystem.jar com.hamersaw.replication_file_system.Controller 15605 3
java -cp build/libs/ReplicationFileSystem.jar com.hamersaw.replication_file_system.ChunkServer /tmp/cs1 localhost 15605 15606
java -cp build/libs/ReplicationFileSystem.jar com.hamersaw.replication_file_system.Client localhost 15605
