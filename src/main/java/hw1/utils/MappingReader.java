package hw1.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Mapping file reader
 */
public class MappingReader {

    /**
     * Static method that reads mapping
     * @param configuration hadoop configuration class
     * @param path hadoop fs path
     * @return Map id -> name
     * @throws IOException when file can not be opened or file has incorrect internal structure
     */
    public static Map<Integer, String> read(Configuration configuration, Path path) throws IOException {
        Pattern splitStr = Pattern.compile(",");
        Map<Integer, String> result = new HashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
        String line;
        int id;
        while ((line = reader.readLine()) != null){
            try {
                String [] data = splitStr.split(line);
                id = Integer.parseInt(data[0]);
                result.put(id, data[1]);
            }
            catch (NumberFormatException noexcept) {
                throw new IOException("Illegal input mapping format");
            }
        }
        return result;
    }
}
