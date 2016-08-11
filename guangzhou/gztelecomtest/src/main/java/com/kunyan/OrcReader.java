package com.kunyan;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Reading orc(SNAPPY) file
 * 参考https://github.com/kshitiz8/hadooppg
 */
public class OrcReader extends Configured  implements Tool{

	public static class MyMapper<K extends WritableComparable, V extends Writable>
            extends MapReduceBase implements Mapper<K, OrcStruct, Text, Text> {

		private StructObjectInspector oip;
		private final OrcSerde serde = new OrcSerde();

		public void configure(JobConf job) {

            Properties table = new Properties();

			serde.initialize(job, table);

			try {
				oip = (StructObjectInspector) serde.getObjectInspector();
			} catch (SerDeException e) {
				e.printStackTrace();
			}
		}

		public void map(K key, OrcStruct val, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String value = val.toString();
            String raw = value.substring(1, value.length() - 1);
            String[] dataArray = raw.split(",");

            String url = dataArray[0];
            String[] visitSearchPatterns = new String[]{"gw.*stock.*?(\\d{6})", "gubaapi.*code=(\\d{6})", "0033.*list.*(\\d{6}).*json", "platform.*symbol=\\w\\w(\\d{6})", "quote.*(\\d{6}).html", "tfile.*(\\d{6})/fs_remind", "dict.hexin.cn.*pattern=(\\S.*)", "suggest.eastmoney.com.*input=(.*?)&", "smartbox.gtimg.cn.*q=(.*?)&", "suggest3.sinajs.cn.*key=(((?!&name).)*)"};
            for (String visitSearchPattern : visitSearchPatterns) {

                Matcher matcher = Pattern.compile(visitSearchPattern).matcher(url);

                if (matcher.find()) {
                    output.collect(new Text("" + value.length() % 100), new Text(calculate(raw, "kunyannn")));
                    break;
                }
            }

		}

        //---------需要取特定字段时需要用到下面两个方法-----------//

        /**
         * 取一个string类型的字段
         * @param fields
         * @param oip
         * @param serde
         * @param val
         * @param index
         * @return
         */
        public static String getStringColumn(List<? extends StructField> fields, StructObjectInspector oip, OrcSerde serde, OrcStruct val, int index) {
            StringObjectInspector inspector =
                    (StringObjectInspector) fields.get(index).getFieldObjectInspector();
            String value = "none";
            try {
                value =  inspector.getPrimitiveJavaObject(oip.getStructFieldData(serde.deserialize(val), fields.get(index)));
            } catch (SerDeException e1) {
                e1.printStackTrace();
            }
            return value;
        }

        /**
         * 取一个bigint型的字段
         * @param fields
         * @param oip
         * @param serde
         * @param val
         * @param index
         * @return
         */
        public static String getLongColumn(List<? extends StructField> fields, StructObjectInspector oip, OrcSerde serde, OrcStruct val, int index) {

            WritableLongObjectInspector wInspector = (WritableLongObjectInspector) fields.get(index).getFieldObjectInspector();
            Long value = -1l;
            try {
                value =  (Long)wInspector.getPrimitiveJavaObject(oip.getStructFieldData(serde.deserialize(val), fields.get(index)));
            } catch (SerDeException e1) {
                e1.printStackTrace();
            }

            return value.toString();
        }

        /**
         * 进行DES和base64加密
         * @param source
         * @param password
         * @return
         */
        public static String calculate(String source, String password) {

            String base64EnStr = "";

            // Encrypt the text
            byte[] textEncrypted = encrypt(source.getBytes(),password);

            if (textEncrypted != null)
                base64EnStr = new BASE64Encoder().encode(textEncrypted);

            return base64EnStr;
        }

        /**
         * DES加密
         * @param dataSource byte[]
         * @param password String
         * @return byte[]
         */
        public static  byte[] encrypt(byte[] dataSource, String password) {
            try{
                SecureRandom random = new SecureRandom();
                DESKeySpec desKey = new DESKeySpec(password.getBytes());
                //创建一个密匙工厂，然后用它把DESKeySpec转换成
                SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
                SecretKey securekey = keyFactory.generateSecret(desKey);
                //Cipher对象实际完成加密操作
                Cipher cipher = Cipher.getInstance("DES");
                //用密匙初始化Cipher对象
                cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
                //现在，获取数据并加密
                //正式执行加密操作
                return cipher.doFinal(dataSource);
            }catch(Throwable e){
                e.printStackTrace();
            }
            return null;
        }

        /**
         * DES解密
         * @param src byte[]
         * @param password String
         * @return byte[]
         * @throws Exception
         */
        public static byte[] decrypt(byte[] src, String password) throws Exception {
            // DES算法要求有一个可信任的随机数源
            SecureRandom random = new SecureRandom();
            // 创建一个DESKeySpec对象
            DESKeySpec desKey = new DESKeySpec(password.getBytes());
            // 创建一个密匙工厂
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            // 将DESKeySpec对象转换成SecretKey对象
            SecretKey securekey = keyFactory.generateSecret(desKey);
            // Cipher对象实际完成解密操作
            Cipher cipher = Cipher.getInstance("DES");
            // 用密匙初始化Cipher对象
            cipher.init(Cipher.DECRYPT_MODE, securekey, random);
            // 真正开始解密操作
            return cipher.doFinal(src);
        }
	}

	public static class MyReducer<K extends WritableComparable, V extends Writable>
            extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output,
				Reporter reporter)
						throws IOException {

			while (values.hasNext()) {
                output.collect(key, values.next());
            }
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new OrcReader(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		JobConf job = new JobConf(new Configuration(), OrcReader.class);

		// Specify various job-specific parameters     
		job.setJobName("STOCK ANALYSIS");

		job.set("hive.io.file.read.all.columns","true");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(OrcReader.MyMapper.class);
		job.setCombinerClass(OrcReader.MyReducer.class);
		job.setReducerClass(OrcReader.MyReducer.class);

		job.setInputFormat(OrcInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		JobClient.runJob(job);
		return 0;
	}

}