package com.hjx.weblog.flume.utils;


import com.hjx.weblog.common.time.TimeTranstationUtils;
import com.hjx.weblog.flume.constant.ConstantFields;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.io.File.separator;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-09 21:18
 */
public class FlumeFileUtils {

   // private static final Logger logger = Logger.getLogger(FlumeFileUtils.class);

    //实现文件解析并返回解析结果
    //返回我们返回2个值，一个是解析的内容，另一个是文件绝对路径 用MAP
    /**
     * @param file  是我们解析的文件
     * @param succPath  是我们文件解析成功存放的目录
     * @return
     */
    public static Map<String,Object> parseFile(File file, String succPath){
        Map<String,Object> mapResult = new HashMap<>();

        //拼接新文件目录
        String fileNew = succPath + separator + TimeTranstationUtils.Date2yyyy_MM_dd() + getDir(file);
        String fileNewName = fileNew + file.getName();

        try {
            if(new File(fileNewName).exists()){
                //如果文件已经存在，不做处理，直接删除
           //     logger.info("文件已经存在，直接删除");
                file.delete();
          //      logger.info("删除文件"+ file.getAbsolutePath() + "成功");
            }else{
                //如果文件不存在，进行解析
                List<String> lines = FileUtils.readLines(file);
                mapResult.put(ConstantFields.VALUE,lines);
                mapResult.put(ConstantFields.ABSOLUTE_FILENAME,fileNewName);
                //处理完成 移动到成功目录
                FileUtils.moveToDirectory(file,new File(fileNew),true);
           //     logger.info("移动文件"+ file.getAbsolutePath() + "到" + fileNew+"成功");
            }
        } catch (IOException e) {
       //     logger.error("文件处理失败");
       //     logger.error(null,e);
        }
        return mapResult;
    }


    //获取文件的目录 从2层开始获取
    /**
     * 获取文件父目录  从第二个目录开始截取
     * @param file
     * @return
     */
    public static String getDir(File file){

        String dir=file.getParent();
        StringTokenizer dirs = new StringTokenizer(dir, separator);
        List<String> list=new ArrayList<String>();
        while(dirs.hasMoreTokens()){
            list.add((String)dirs.nextElement());
        }
        String str="";
        for(int i=2;i<list.size();i++){
            str=str+separator+list.get(i);
        }
        return str+"/";
    }


    public static void main(String[] args) {
       /* String dir = FlumeFileUtils.getDir(new File("F:\\tzjy教学课件\\讲课内容\\20190809 flume实时数据解析\\资料\\测试数据\\wechat\\wechat_source1_1111168.txt"));
        System.out.println(dir);*/
        FlumeFileUtils.parseFile(new File("F:\\tzjy教学课件\\讲课内容\\20190809 flume实时数据解析\\资料\\测试数据\\wechat\\wechat_source1_1111168.txt"),"F:\\tzjy教学课件\\讲课内容\\20190809 flume实时数据解析\\资料\\测试数据\\test");
    }

}
