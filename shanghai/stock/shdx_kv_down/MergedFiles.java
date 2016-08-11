import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;


/**
 * Created by liaochengming on 2015-12-14.
 * 此是用来检测指定文件夹下的数据文件，并合并数据文件
 */

class MergedFiles {

    /**
     * 此方法用来合并数据文件
     *
     * @param outFile 合并文件后的输出文件路径
     * @param files   需要合并的多个文件
     * @return 是否合并成功
     */
    private static boolean mergeFiles(String outFile, String[] files) {

        FileChannel outChannel = null;

        try {

            outChannel = new FileOutputStream(outFile).getChannel();

            for (String file : files) {

                FileChannel fileChannel = new FileInputStream(file).getChannel();
                ByteBuffer byteBuffer = ByteBuffer.allocate(8192);

                while (fileChannel.read(byteBuffer) != -1) {

                    byteBuffer.flip();
                    outChannel.write(byteBuffer);
                    byteBuffer.clear();

                }

                fileChannel.close();
            }

            return true;

        } catch (IOException ioException) {

            ioException.printStackTrace();
            return false;

        } finally {

            try {

                if (outChannel != null) {

                    outChannel.close();

                }

            } catch (IOException ioException) {

                ioException.printStackTrace();

            }
        }
    }

    static void doMer() {

        String line = readFirstLine("/home/telecom/shdx/bin/shdx_kv_down/write_over.txt");

        if (!line.equals("false")) {

            int lineNum = Integer.valueOf(line.substring(4));

            if (lineNum > 12) {

                merged("/home/liaochengming/shdx/kv_down_files/", "/home/telecom/shdx/data/search/temp/", "/home/telecom/shdx/bin/shdx_kv_down/write_over.txt");

            }
        }

    }

    /**
     * 读取文件的第一行
     *
     * @param filepath 需要读取的文件路径
     * @return 读取的数据
     */
    private static String readFirstLine(String filepath) {

        File file = new File(filepath);

        if (file.exists()) {

            String sign;
            BufferedReader reader = null;

            try {

                reader = new BufferedReader(new FileReader(file));
                String tempString;

                tempString = reader.readLine();
                reader.close();

                if (null == tempString) {

                    sign = "false";
                    return sign;

                }

                sign = tempString;
                return sign;

            } catch (IOException ioException) {

                ioException.printStackTrace();
                sign = "false";
                return sign;

            } finally {

                if (reader != null)

                    try {

                        reader.close();

                    } catch (IOException ioException) {

                        ioException.printStackTrace();

                    }
            }
        }

        return "false";
    }


    /**
     * 合并完文件后，将false写到记录的文件里
     *
     * @param overPath 下载的记录文件路径
     */
    private static void writeFalse(String overPath) {

        File file = new File(overPath);
        BufferedWriter writer = null;

        try {

            writer = new BufferedWriter(new FileWriter(file));
            writer.write("false");
            writer.close();

        } catch (IOException ioException) {

            ioException.printStackTrace();

        } finally {

            if (writer != null)

                try {

                    writer.close();

                } catch (IOException ioException) {

                    ioException.printStackTrace();

                }
        }
    }

    /**
     * 执行合并文件
     *
     * @param files_path 需要合并的文件路径
     * @param out_path   合并后文件存放的路径
     * @param overPath   记录下载的文件路径
     */
    private static void merged(String files_path, String out_path, String overPath) {

        File file = new File(files_path);
        File[] array = file.listFiles();

        if ((null != array) && (array.length != 0)) {

            String[] fileNames = new String[array.length];
            String outfileName = array[0].getName().substring(0, 10);

            for (int i = 0; i < array.length; ++i) {

                fileNames[i] = files_path + array[i].getName();

            }

            File outFile = new File(out_path);

            if (!(outFile.exists())) {

                outFile.mkdirs();

            }

            boolean over = mergeFiles(out_path + outfileName + ".txt", fileNames);

            if (over) {

                delete(fileNames);
                writeFalse(overPath);

            }
        }
    }

    /**
     * 此方法用来删除文件
     *
     * @param fileNames 需要删除的文件路径
     */
    private static void delete(String[] fileNames) {

        for (String fileName : fileNames) {

            File file = new File(fileName);

            if (file.exists()) {

                file.delete();

            }
        }
    }
}