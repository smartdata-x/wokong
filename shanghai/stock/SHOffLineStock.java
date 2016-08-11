import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by liaochengming on 2016-07-20
 * 此类是上海电信
 * 用来获取股票离线的查看和搜索量
 */
public class VisitAndSearch {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {

            System.err.println("Usage: TimeAndUrl <in> [<in>...] <out>");
            System.exit(2);

        }

        Job job = new Job(conf, "VisitAndSearch");
        job.setJarByClass(VisitAndSearch.class);
        job.setMapperClass(VisitAndSearch.TokenizerMapper.class);
        job.setReducerClass(VisitAndSearch.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));
        System.exit((job.waitForCompletion(true)) ? 0 : 1);

    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            int i = 1;
            Text newKey = new Text();
            Iterator localIterator = values.iterator();

            while (localIterator.hasNext()) {

                newKey.set(key.toString() + i);
                Text val = (Text) localIterator.next();
                context.write(newKey, val);
                ++i;

            }
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text value = new Text();
        private Text key = new Text();
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] datas = value.toString().split("\\t");

            if (datas.length >= 4) {

                String timeStr = datas[2];
                String url = datas[3];

                String[] visitSearchPatterns = {

                        // 搜索
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
                        "www.yicai.com.*searchKeyWords=(.*)",
                        "www.gw.com.cn.*q=(.*)&s=",
                        "cailianpress.com/search.*keyword=(.*)",
                        "api.cailianpress.com.*PutinInfo=(.*)&sign",
                        "news.21cn.com.*keywords=(.*)&view",
                        "news.10jqka.com.cn.*text=(.*)&jsoncallback",
                        "smartbox.gtimg.cn.*q=(.*?)&",
                        "swww.niuguwang.com/stock/.*q=(.*?)&",
                        "info.zq88.cn:9085.*query=(\\d{6})&",

                        // 查看
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
                        "guba.eastmoney.com.*code=.*(\\d{6})",
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
                        "gu.qq.com.*(\\d{6})\\?",
                        "gubaapi.*code=.*(\\d{6})",
                        "mnews.gw.com.cn.*(\\d{6})/(list|gsgg|gsxw|yjbg)",
                        "101.226.68.82.*code=.*(\\d{6})",
                        "183.238.123.235.*code=(\\d{6})&",
                        "zszx.newone.com.cn.*code=(\\d{6})",
                        "compinfo.hsmdb.com.*stock_code=(\\d{6})",
                        "mt.emoney.cn.*barid=(\\d{6})",
                        "news.10jqka.com.cn/stock_mlist/(\\d{6})",
                        "www.newone.com.cn.*code=(\\d{6})",
                        "219.141.183.57.*gpdm=(\\d{6})&",
                        "www.hczq.com.*stockCode=(\\d{6})",
                        "open.hs.net.*en_prod_code=(\\d{6})",
                        "stock.pingan.com.cn.*secucodes=(\\d{6})",
                        "58.63.254.170:7710.*code=(\\d{6})&",
                        "sjf10.westsecu.com/stock/(\\d{6})/",
                        "mds.gf.com.cn.*/(\\d{6})",
                        "211.152.53.105.*key=(\\d{6}),"
                };

                for (int i = 0; i < visitSearchPatterns.length; ++i) {

                    Matcher matcher = Pattern.compile(visitSearchPatterns[i]).matcher(url);

                    if (matcher.find()) {

                        if (matcher.group(1).getBytes().length > 6000) {

                            return;

                        } else {

                            this.value.set(matcher.group(1) + "\t" + timeStr + '\t' + String.valueOf(i));
                            this.key = getKey(timeStr);
                            context.write(this.key, this.value);
                            return;

                        }
                    }
                }
            }
        }

        /**
         * 此类用来格式化时间
         *
         * @param timeStr 时间字符串
         * @return 格式化后的时间字符串
         */
        public Text getKey(String timeStr) {

            long time = Long.valueOf(timeStr).longValue();
            Date dt = new Date(time);
            String sDateTime = this.sdf.format(dt);
            return new Text(sDateTime);

        }
    }
}