/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-17 14:38
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_COMMON_UTIL_H_
#define KMONITOR_CLIENT_COMMON_UTIL_H_

#include <cstdlib>
#include <iostream>
#include <string>

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class Util {
public:
    static bool GetHostIP(std::string &host);
    static int64_t TimeAlign(int64_t curTime, int64_t timeBucket);
    static bool HostnameToIp(std::string hostname, std::string &ip);
    static const std::string GetMachineName();

private:
    Util(const Util &);
    Util &operator=(const Util &);

    static const std::string getMachineName(std::istream &is);

private:
    static const int BUF_SIZE = 1000;
    static const std::string HOST_INFO_PATH;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_COMMON_UTIL_H_
