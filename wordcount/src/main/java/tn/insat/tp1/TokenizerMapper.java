package tn.insat.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

    private Text magasin = new Text();
    private FloatWritable cout = new FloatWritable();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] columns = value.toString().split(",");

        if (columns.length == 7) {
            // Extract the 'magasin' and 'cout' values
            magasin.set(columns[3]);
            cout.set(Float.parseFloat(columns[5]));
            System.out.println(magasin);
            System.out.println(cout);
            context.write(magasin, cout);
        }
    }
}

