
package gzipToLzo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;

import com.hadoop.compression.lzo.LzopCodec;

public class GzipToLzo {
	public static void main(String[] args) throws IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path("/yueyetest/yueye12.lzo");
	
		LzopCodec c = new LzopCodec();
		c.setConf(conf);		
		
		//read
	byte[] b = new byte[1024];             		
		//gzip
		GzipCodec g = new GzipCodec();
		g.setConf(conf);
		InputStream gin = g.createInputStream(fs.open(new Path("/yueyetest/yueye.txt.gz")));
//		FileInputStream in = new FileInputStream(new File("/mnt/yueye.txt"));
//	int len = in.read(b);		
	int glen = gin.read(b);
	OutputStream os = c.createOutputStream(fs.create(file));
//	os.write(b, 0, len);
	os.write(b, 0, glen);
	os.close();
	}
}
