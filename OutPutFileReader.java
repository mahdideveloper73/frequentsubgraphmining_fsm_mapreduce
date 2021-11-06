import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class OutPutFileReader {
    public static void printResult() {

        Configuration conf = new Configuration();
        try {
            Collection<File> files = FileUtils.listFiles(new File("C:\\out7"), new RegexFileFilter("^(.*?)"), DirectoryFileFilter.DIRECTORY);
            List<File> collect = files.stream().filter(i -> i.getName().startsWith("part-") && i.getPath().startsWith("C:\\out7\\output-graph-")).collect(Collectors.toList());
            int i = 1;
            for (File file : collect) {
                Path inFile = new Path(file.getPath());
                Reader reader = null;
                try {
                    Text key = new Text();
                    BytesWritable value = new BytesWritable();
                    reader = new Reader(conf, Reader.file(inFile), Reader.bufferSize(4096));

                    while (reader.next(key, value)) {
                        ObjectInput input = new ObjectInputStream(new ByteArrayInputStream(value.getBytes()));
                        MR_Serialize mr_serialize = (MR_Serialize) input.readObject();
                        System.out.println(i +"= Key ==> " + key + " Value ==> " + mr_serialize.Pattern.support);
                        i++;
                    }

                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            // TODO Auto-generated catch bloc
            e.printStackTrace();
        }
    }
}