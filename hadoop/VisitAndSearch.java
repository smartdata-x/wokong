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
            String[] visitSearchPatterns = new String[]{
               //搜索
                    "quotes.money.163.com.*word=(.*)&t=",
                    "quotes.money.163.com.*word=(.*)&count",
                    "suggest3.sinajs.cn.*key=(((?!&name).)*)",
                    "xueqiu.com.*code=(.*)&size",
                    "xueqiu.com.*q=(.*)",
                    "app.stcn.com.*&wd=(.*)",
                    "q.ssajax.cn.*q=(.*)&type",
                    "znsv.baidu.com.*wd=(.*)&ch",
                    "so.stockstar.com.*q=(.*)&click",
                    "quote.cfi.cn.*keyword=(.*)&his",
                    "hq.cnstock.com.*q=(.*)&limit",
                    "xinpi.cs.com.cn.*q=(.*)",
                    "xinpi.cs.com.cn.*c=(.*)&q",
                    "search.cs.com.cn.*searchword=(.*)",
                    "d.10jqka.com.cn.*(\\d{6})/last",
                    "news.10jqka.com.cn.*keyboard_(\\d{6})",
                    "10jqka.com.cn.*w=(.*)&tid",
                    "data.10jqka.com.cn.*/(\\d{6})/",
                    "dict.hexin.cn.*pattern=(\\d{6})",
                    "dict.hexin.cn.*pattern=(\\S.*)",
                    "q.stock.sohu.com.*keyword=(.*)&(_|c)",
                    "api.k.sohu.com.*words=(.*)&p1",
                    "hq.p5w.net.*query=(.*)&i=",
                    "code.jrjimg.cn.*key=(.*)&d=",
                    "itougu.jrj.com.cn.*keyword=(.*)&is_stock",
                    "wallstreetcn.com/search\\?q=(.*)",
                    "api.markets.wallstreetcn.com.*q=(.*)&limit",
                    "so.hexun.com.*key=(.*)&type",
                    "finance.ifeng.com.*code.*(\\d{6})",
                    "finance.ifeng.com.*q=(.*)&cb",
                    "suggest.eastmoney.com.*input=(.*?)&",
                    "so.eastmoney.com.*q=(.*)&m",
                    "guba.eastmoney.com/search.aspx?t=(.*)",
                    "guba.eastmoney.com.*code=(\\d{6})",
                    "www.yicai.com.*searchKeyWords=(.*)",
                    "www.gw.com.cn.*q=(.*)&s=",
                    "cailianpress.com/search.*keyword=(.*)",
                    "api.cailianpress.com.*PutinInfo=(.*)&sign",
                    "news.21cn.com.*keywords=(.*)&view",
                    "news.10jqka.com.cn.*text=(.*)&jsoncallback",
                    "smartbox.gtimg.cn.*q=(.*?)&",
  
    		  //查看
    		  "quotes.money.163.com/app/stock/\\d(\\d{6})",
    		  "m.news.so.com.*q=(\\d{6})",
    		  "finance.sina.com.cn.*(\\d{6})/nc.shtml",
    		  "guba.sina.com.cn.*name=.*(\\d{6})",
    		  "platform.*symbol=\\w\\w(\\d{6})",
    		  "xueqiu.com/S/.*(\\d{6})",
    		  "cy.stcn.com/S/.*(\\d{6})",
    		  "mobile.stcn.com.*secucode=(\\d{6})",
    		  "stock.quote.stockstar.com/(\\d{6}).shtml",
    		  "quote.cfi.cn.*searchcode=(.*)",
    		  "hqapi.gxfin.com.*code=(\\d{6}).sh",
    		  "irm.cnstock.com.*index/(\\d{6})",
    		  "app.cnstock.com.*k=(\\d{6})",
    		  "stockpage.10jqka.com.cn/(\\d{6})/",
    		  "0033.*list.*(\\d{6}).*json",
    		  "q.stock.sohu.com/cn/(\\d{6})/",
    		  "s.m.sohu.com.*/(\\d{6})",
    		  "data.p5w.net.*code.*(\\d{6})",
    		  "hq.p5w.net.*a=.*(\\d{6})",
    		  "jrj.com.cn.*(\\d{6}).s?html",
    		  "mapi.jrj.com.cn.*stockcode=(\\d{6})",
    		  "api.buzz.wallstreetcn.com.*(\\d{6})&cid",
    		  "stockdata.stock.hexun.com/(\\d{6}).shtml",
    		  "finance.ifeng.com/app/hq/stock.*(\\d{6})",
    		  "api.3g.ifeng.com.*k=(\\d{6})",
    		  "quote.*(\\d{6}).html",
    		  "gw.*stock.*?(\\d{6})",
    		  "tfile.*(\\d{6})/fs_remind",
    		  "api.cailianpress.com.*(\\d{6})&Sing",
    		  "stock.caijing.com.cn.*(\\d{6}).html",
    		  "qt.gtimg.cn.*q=.*(\\d{6})",
    		  "finance.qq.com.*(\\d{6}).shtml",
    		  "gubaapi.*code=(\\d{6})",
            }
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
