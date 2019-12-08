package com.hjx.weblog.flume.source;

/**
 * Created by Axin in 2019/11/12 22:35
 */

import com.hjx.weblog.flume.constant.ConstantFields;
import com.hjx.weblog.flume.utils.FlumeFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 *
 * flume用户文档 ：
 * http://flume.apache.org/releases/content/1.9.0/FlumeUserGuide.html
 * 自定义Source
 * 文档 ：http://flume.apache.org/releases/content/1.9.0/FlumeDeveloperGuide.html#source
 *
 *
 * #jar包及依赖安装目录（此目录在cdh中的flume配置中可以查看。）
 * /var/lib/flume-ng/plugins.d/chl/lib
 *
 * #flume监控目录（在 cdh中flume配置及查看，此目录其实就是flume普通安装方式中安装目录中的flume.conf）
 * /usr/chl/data/filedir/
 *
 */
public class FolderSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger logger = Logger.getLogger(FolderSource.class);

    private int filenum;
    private Collection<File> allFiles ; //总文件数
    private List<File> listFiles;       //每次轮询处理的文件数
    private String filePath;            //监控目录
    private String successDir;          //文件处理成功写入的目录
    private List<Event> eventList;

    /**
     * 读取配置文件 flume.conf
     * @param context
     */
    @Override
    public void configure(Context context) {
        filePath = context.getString("filePath");
        filenum = context.getInteger("filenum");
        successDir = context.getString("successfulDir");
        eventList = new ArrayList<>();
    }

    /**
     * 主要实现业务在这里
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        //TODO 定义所有处理逻辑 这个方法会不断轮询

        try {
            Thread.currentThread().sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Status status = null;
        //监控目录，拿到每批需要处理的文件数
        try {
            allFiles = FileUtils.listFiles(new File(filePath), new String[]{"txt","bcp"}, true);

            logger.info("allFiles=====" + allFiles.size());

            //每次只处理多少个文件，避免文件数过大
            if(allFiles.size() > filenum){
                listFiles = ((List<File>) allFiles).subList(0, filenum);
                logger.info("listFiles=====" + listFiles.size());
            }else{
                listFiles = ((List<File>) allFiles);
                logger.info("listFiles=====" + listFiles.size());
            }

            //对每批文件进行遍历解析，解析成功的写入成功目录，失败的写入失败的目录
            if(listFiles.size() > 0){
                listFiles.forEach(file->{
                    //获取文件名
                    String fileName = file.getName();
                    //对文件进行解析
                    Map<String, Object> stringObjectMap = FlumeFileUtils.parseFile(file, successDir);
                    List<String> lines = (List<String>)stringObjectMap.get(ConstantFields.VALUE);
                    String absolute_filename = (String)stringObjectMap.get(ConstantFields.ABSOLUTE_FILENAME);

                    lines.forEach(line->{
                        Map<String,String> header = new HashMap<>();
                        header.put(ConstantFields.FILENAME,fileName);
                        header.put(ConstantFields.ABSOLUTE_FILENAME,absolute_filename);
                        Event event = new SimpleEvent();
                        event.setBody(line.getBytes());
                        event.setHeaders(header);
                        eventList.add(event);
                    });
                });

                //TODO 批量推送
                if(eventList.size()>0){
                    getChannelProcessor().processEventBatch(eventList);
                    logger.info("批量推送数据到拦截器" + eventList.size() +"条数据");
                    eventList.clear();
                }

            }
            status = Status.READY;
        } catch (Exception e) {
            status = Status.BACKOFF;
            logger.error("解析异常",e);
        }finally {
            eventList.clear();
        }
        return status;
    }


    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
