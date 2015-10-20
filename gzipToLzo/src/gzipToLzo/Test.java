package gzipToLzo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test{

  public static void main(String[] args) throws IOException {
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path src = new Path("/home/word2222.txt");
    Path dst = new Path("/2015-10-17/");
    fs.copyFromLocalFile(src, dst);
    fs.close();
  }
}
