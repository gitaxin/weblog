package com.hjx.weblog.spark.warn.timer;


import com.hjx.weblog.spark.warn.domain.WarningMessage;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-23 21:04
 */
public class PhoneWarn implements WarnI {
    @Override
    public boolean warn(WarningMessage message) {
        //手机号多个以"，"分割 群发
        String[] sendMobiles = message.getSendMobile().split(",");
        for (int i = 0; i < sendMobiles.length; i++) {
            //获取告警内容
            String senfInfo = message.getSenfInfo();
            //调用第三方短信接口
            MessageSend.sendMessage(sendMobiles[i],senfInfo);
            System.out.println("发送消息" +message.toString() + "成功" );
        }
        //调用短信接口
        return false;
    }
}
