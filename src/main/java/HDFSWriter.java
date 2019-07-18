import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSWriter {
    static FileSystem fs = null;
    static final Path apacheLogFile = new Path("/tmp/apache.access.log");

    static void write(String message) throws IOException {
        FileSystem fs  = HDFSWriter.getFileSystem();
        FSDataOutputStream outputStream;
        if (!fs.exists(apacheLogFile)) {
            outputStream = fs.create(apacheLogFile);
        }else {
            outputStream = fs.append(apacheLogFile);
        }
        outputStream.write(message.getBytes());
        LoggerFactory
                .getLogger("consumer.hdfs.logger")
                .info(String.format("%d BYTES WAS WRITTEN WITH SUCCESS", outputStream.size()));
        outputStream.close();
    }

    static FileSystem getFileSystem() throws IOException {
        if (fs != null) {
            return fs;
        }
        Configuration conf = new Configuration();
        conf.addResource(
                new Path(
                        String.join("/", System.getProperty("hadoop.conf_dir"), "core-site.xml")
                )
        );
        conf.addResource(
                new Path(
                        String.join("/", System.getProperty("hadoop.conf_dir"), "hdfs-site.xml")
                )
        );
        fs = FileSystem.newInstance(conf);
        return fs;
    }
}
