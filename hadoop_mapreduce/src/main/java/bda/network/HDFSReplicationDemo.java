package bda.network;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS Replication Demonstration via Hadoop Java API
 * Directly aligns with course performance indicator PI-2.2.1.1 & Replication-Example.doc
 * Demonstrates:
 * 1. Configuring dfs.replication dynamically before creating files.
 * 2. Changing the replication factor on an existing HDFS file using fs.setReplication().
 * 3. Inspecting block locations and replica status.
 * Course: Big Data Analytics (26ECSC404) - KLE Technological University
 */
public class HDFSReplicationDemo {

    public static void main(String[] args) {
        String hdfsUri = args.length > 0 ? args[0] : "hdfs://localhost:9000";
        String filePathStr = args.length > 1 ? args[1] : "/network/replication_demo/sample_traffic.txt";

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUri);

            // Step 1: Set global default replication factor to 1 in configuration
            conf.set("dfs.replication", "1");
            System.out.println("[+] Configured default dfs.replication = 1");

            FileSystem fs = FileSystem.get(conf);
            Path destPath = new Path(filePathStr);

            // Ensure parent directory exists
            if (!fs.exists(destPath.getParent())) {
                fs.mkdirs(destPath.getParent());
            }

            // Step 2: Write sample data with replication factor 1
            System.out.println("[+] Writing sample network log to HDFS: " + destPath);
            FSDataOutputStream out = fs.create(destPath, true);
            out.writeUTF("2026-09-24T20:10:00,192.168.1.10,10.0.0.5,443,52341,TCP,20,14250,1.4,ACK,BENIGN\n");
            out.writeUTF("2026-09-24T20:10:05,198.51.100.42,10.0.0.5,80,51120,TCP,1500,90000,0.5,SYN,DDoS_FLOOD\n");
            out.close();

            // Step 3: Inspect initial file replication factor & block info
            FileStatus status = fs.getFileStatus(destPath);
            System.out.println("--------------------------------------------------");
            System.out.println("File: " + status.getPath());
            System.out.println("Size: " + status.getLen() + " bytes");
            System.out.println("Initial Replication Factor: " + status.getReplication());
            System.out.println("Block Size: " + (status.getBlockSize() / (1024 * 1024)) + " MB");

            BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
            for (int i = 0; i < blocks.length; i++) {
                System.out.println("  Block " + i + " Hosts: " + String.join(", ", blocks[i].getHosts()));
            }

            // Step 4: Dynamically modify replication factor to 2 using Java API
            short newReplicationFactor = 2;
            System.out.println("\n[+] Dynamically modifying replication factor to " + newReplicationFactor + "...");
            boolean success = fs.setReplication(destPath, newReplicationFactor);

            if (success) {
                System.out.println("[SUCCESS] fs.setReplication() call succeeded.");
                FileStatus updatedStatus = fs.getFileStatus(destPath);
                System.out.println("Updated Replication Factor in NameNode metadata: " + updatedStatus.getReplication());
            } else {
                System.out.println("[FAILED] Could not update replication factor.");
            }

            System.out.println("--------------------------------------------------");
            System.out.println("Note: On a single-node pseudo-distributed cluster, NameNode marks");
            System.out.println("the block as UNDER-REPLICATED until additional DataNodes join.");
            System.out.println("Check via CLI: hdfs fsck " + destPath + " -files -blocks -locations");

            fs.close();
        } catch (IOException e) {
            System.err.println("[-] HDFS Replication Demo Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
