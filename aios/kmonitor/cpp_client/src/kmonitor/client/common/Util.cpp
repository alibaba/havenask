/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-17 14:38
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/common/Util.h"

#include <arpa/inet.h>
#include <fstream>
#include <istream>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "autil/Regex.h"

using std::string;

BEGIN_KMONITOR_NAMESPACE(kmonitor);

// IGRAPH_LOG_SETUP(client, Util);

const string Util::HOST_INFO_PATH("/etc/hostinfo");

bool Util::GetHostIP(string &host) {
    char hname[128];
    gethostname(hname, sizeof(hname));
    struct hostent *hent = gethostbyname(hname);
    if (hent == NULL) {
        return false;
    }

    host = inet_ntoa(*(struct in_addr *)(hent->h_addr_list[0]));
    return true;
}

bool Util::HostnameToIp(string hostname, string &ip) {
    struct hostent *he;
    struct in_addr **addr_list;
    if ((he = gethostbyname(hostname.c_str())) == NULL) {
        return false;
    }
    addr_list = (struct in_addr **)he->h_addr_list;
    for (int i = 0; addr_list[i] != NULL; i++) {
        char temp[Util::BUF_SIZE];
        // Return the first one;
        snprintf(temp, sizeof(temp), "%s", inet_ntoa(*addr_list[i]));
        ip = temp;
        return true;
    }
    return false;
}

const string Util::GetMachineName() {
    string line;
    std::ifstream hostinfo(HOST_INFO_PATH.c_str());
    if (!hostinfo.is_open()) {
        return line;
    }
    line = getMachineName(hostinfo);
    hostinfo.close();
    return line;
}

const string Util::getMachineName(std::istream &is) {
    string line;
    while (getline(is, line)) {
        if (line.length() > 0) {
            return line;
        }
    }
    return "";
}

int64_t Util::TimeAlign(int64_t curTime, int64_t timeBucket) {
    if (timeBucket > 0) {
        return curTime - curTime % timeBucket;
    }
    return curTime;
}

END_KMONITOR_NAMESPACE(kmonitor);
