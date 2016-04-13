//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 此类是上海电信和江苏电信
 * 用来获取股票的查看和搜索量
 */
public class VisitAndSearch {


    public VisitAndSearch() {

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        if (args.length < 2) {
            System.err.println("Usage: TimeAndUrl <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "VisitAndSearch");
        job.setJarByClass(VisitAndSearch.class);
        job.setMapperClass(VisitAndSearch.TokenizerMapper.class);
        job.setCombinerClass(VisitAndSearch.IntSumReducer.class);
        job.setReducerClass(VisitAndSearch.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator var = values.iterator();

            while (var.hasNext()) {
                Text val = (Text) var.next();
                context.write(key, val);
            }

        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text time = new Text();

        private Text stockCode = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            st.nextToken();
            st.nextToken();
            String timeStr = st.nextToken();
            String url = st.nextToken();
            String[] visitSearchPatterns = new String[]{"gw.*stock.*?(\\d{6})", "gubaapi.*code=(\\d{6})", "0033.*list.*(\\d{6}).*json", "platform.*symbol=\\w\\w(\\d{6})", "quote.*(\\d{6}).html", "tfile.*(\\d{6})/fs_remind", "dict.hexin.cn.*pattern=(\\S.*)", "suggest.eastmoney.com.*input=(.*?)&", "smartbox.gtimg.cn.*q=(.*?)&", "suggest3.sinajs.cn.*key=(((?!&name).)*)"};

            for (int i = 0; i < visitSearchPatterns.length; ++i) {
                Matcher matcher = Pattern.compile(visitSearchPatterns[i]).matcher(url);
                if (matcher.find()) {
                    this.stockCode.set(matcher.group(1));
                    this.time.set(timeStr + '\t' + i);
                    context.write(this.stockCode, this.time);
                    break;
                }
            }

        }
    }
}
