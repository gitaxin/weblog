package com.hjx.weblog.spark.warn.timer;


import com.hjx.weblog.spark.warn.domain.WarningMessage;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-23 21:10
 */
public class WechatWarn implements WarnI {


    @Override
    public boolean warn(WarningMessage arningMessage) {
        return false;
    }
}
