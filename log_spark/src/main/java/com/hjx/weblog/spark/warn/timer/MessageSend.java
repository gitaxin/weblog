package com.hjx.weblog.spark.warn.timer;

/**
 * @author: KING
 * @description: 第三方接口
 * @Date:Created in 2019-08-23 21:08
 */
public class MessageSend {
     public static boolean sendMessage(String phone,String content){
         System.out.println(content);
         return true;
     }
}
