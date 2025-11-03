package com.alibaba.search.swift;

/**
 * Created by sdd on 17-3-16.
 */
public class SwiftTestConf {
    public static String zkPath = "zkPath=zfs://10.101.193.222:16181,10.101.193.223:16181,10.101.193.224:16181/swift/swift_service_7u";
    public static String zkPathHttp = "zkPath=http://10.101.193.244:3066/swift_daily";
    public static String logPath = "logConfigFile=./src/main/resources/linux-x86-64/swift_alog.conf";
    public static String otherConf = "useFollowerAdmin=true";

    public static String getSwiftConfig() {
        return zkPath +";" +logPath +";"+otherConf;
    }

}
