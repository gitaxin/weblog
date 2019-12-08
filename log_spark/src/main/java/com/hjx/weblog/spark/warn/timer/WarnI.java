package com.hjx.weblog.spark.warn.timer;


import com.hjx.weblog.spark.warn.domain.WarningMessage;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-23 21:03
 */
public interface WarnI {
    public boolean warn(WarningMessage arningMessage);
}
