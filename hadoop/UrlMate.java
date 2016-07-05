
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlMate {

    public static void main(String[] args) {



        String[] visitSearchPatterns = {
                //搜索
                "quotes.money.163.com.*word=(.*)&t=",
                "quotes.money.163.com.*word=(\\d{6})&count",
                "suggest3.sinajs.cn.*key=(((?!&name).)*)",
                "xueqiu.com.*code=(.*)&size",
                "xueqiu.com.*q=(.*)",
                "app.stcn.com.*&wd=(.*)",
                "q.ssajax.cn.*q=(.*)&type",
                "znsv.baidu.com.*wd=(.*)&ch",
                "so.stockstar.com.*q=(.*)&click",
                "quote.cfi.cn.*keyword=(.*)&his",
                "hq.cnstock.com.*q=(.*)&limit",
                "xinpi.cs.com.cn.*q=(\\d{6})",
                "xinpi.cs.com.cn.*c=(.*)&q",
                "search.cs.com.cn.*searchword=(\\d{6})",
                "d.10jqka.com.cn.*(\\d{6})/last",
                "news.10jqka.com.cn.*keyboard_(\\d{6})",
                "10jqka.com.cn.*w=(\\d{6})",
                "data.10jqka.com.cn.*/(\\d{6})/",
                "dict.hexin.cn.*pattern=(\\d{6})",
                "dict.hexin.cn.*pattern=(\\S.*)",
                "q.stock.sohu.com.*keyword=(.*)&(_|c)",
                "api.k.sohu.com.*words=(.*)&p1",
                "hq.p5w.net.*query=(.*)&i=",
                "code.jrjimg.cn.*key=(.*)&d=",
                "itougu.jrj.com.cn.*keyword=(\\d{6})",
                "wallstreetcn.com/search\\?q=(.*)",
                "api.markets.wallstreetcn.com.*q=(.*)&limit",
                "so.hexun.com.*key=(.*)&type",
                "finance.ifeng.com.*code.*(\\d{6})",
                "finance.ifeng.com.*q=(.*)&cb",
                "suggest.eastmoney.com.*input=(.*?)&",
                "so.eastmoney.com.*q=(\\d{6})",
                "guba.eastmoney.com.*t=(\\d{6})",
                "guba.eastmoney.com.*code=(\\d{6})",
                "www.yicai.com.*searchKeyWords=(\\d{6})",
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
                "gu.qq.com.*(\\d{6})\\?",
                "gubaapi.*code=.*(\\d{6})"
        };

        BufferedReader bf;
        ArrayList<Integer> al = new ArrayList<>();
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(args[0]), "UTF-8");
            bf = new BufferedReader(isr);

            String url;

            while ((url = bf.readLine())!=null) {
                for (int i = 0; i < visitSearchPatterns.length; ++i)
                {
                    Matcher matcher = Pattern.compile(visitSearchPatterns[i]).matcher(url);

                    if (matcher.find())
                    {
                        al.add(i);

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Collections.sort(al);
        System.out.println(al);
    }

}
