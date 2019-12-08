package com.hjx.weblog.flume.source;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * Created by Axin in 2019/11/12 22:38
 */
public class FolderSourceTest {


    public static int filenum = 2;

    public static Collection<File> allFiles ; //总文件数
    public static List<File> listFiles;       //每次轮询处理的文件数

    public static void main(String[] args) {

        //监控目录，拿到每批需要处理的文件数
        String filePath = "E:\\教程\\大数据\\03-企业网络日志项目用户行为分析（king老师）\\0809-Spark Streaming 整合持久化\\资料\\测试数据\\wechat";
        allFiles = FileUtils.listFiles(new File(filePath), new String[]{"txt","bcp"}, true);

        //每次只处理多少个文件，避免文件数过大
        if(allFiles.size() > filenum){
            listFiles = ((List<File>) allFiles).subList(0, filenum);
        }else{
            listFiles = ((List<File>) allFiles);
        }

        //对每批文件进行遍历解析，解析成功的写入成功目录，失败的写入失败的目录


        System.out.println(listFiles.size());

    }
}
