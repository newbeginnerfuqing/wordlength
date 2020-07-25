import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordlength {
    //map函数，执行被分化后的小型任务
    public static class wordlengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private String pattern = "[^a-zA-Z0-9-]";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 将文本分化成一个一个单词进行计算
            line = line.replaceAll(pattern, " "); 
            String[] words = line.split("\\s+");
            for (String word : words) {
                if (word.length() > 0) { //获取单词长度并记录
                    context.write( new IntWritable(word.length()), new IntWritable(1));
                }
            }
        }
    }

    //reduce函数，归并数据，进行处理得出结果
    public static class wordlengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
                    Integer count = 0;
                    for (IntWritable value : values) {
                            count += value.get();
                    }
                    // 
                    context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "wordlength");
        // 设置运行Job的类
        job.setJarByClass(wordlength.class);
        // 设置Mapper类
        job.setMapperClass(wordlengthMapper.class);
        // 设置Reducer类
        job.setReducerClass(wordlengthReducer.class);
        // 设置Map输出的Key value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置Reduce输出的Key value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if (!b) {
            System.out.println("wordlength task fail!");
        }
    }
}