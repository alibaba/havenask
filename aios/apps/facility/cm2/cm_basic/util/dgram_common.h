/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * =====================================================================================
 *
 *       Filename:  dgram_client.h
 *
 *    Description:  ClusterMap库的功能类，UDP通信的公共代码，主要是创建socket和socket地址
 *
 *        Version:  0.1
 *        Created:  2013-05-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __CM_BASIC_DGRAM_COMMON_H__
#define __CM_BASIC_DGRAM_COMMON_H__

#include <netinet/in.h>

#include "autil/Log.h"

/* hostname的最大长度 */
// const static int HOST_SIZE = 256;
/* udp消息的最大长度 */
// const static int BUF_SIZE = 1500;

namespace cm_basic {

const int32_t MAX_UDP_PACKET_SIZE = 4096;

class CDGramCommon
{
public:
    /* 创建服务端socket，输入参数是端口号 */
    int make_dgram_server_socket(int);

    /* 创建客户端socket */
    int make_dgram_client_socket(void);

    /* 根据ip地址和端口，创建socket地址 */
    int make_internet_address(const char*, int, sockaddr_in*);

    /* 根据socket地址，获取ip地址和端口 */
    int get_internet_address(char*, int, int*, sockaddr_in*);

    /*
     * function：取得本地的IP地址字符串表示，因为可能有多个网址，所以对应。
     * para: nSize返回数组的长度
     * return： 成功返回多个IP地址，失败返回NULL，另外返回的地址由调用者释放
     */
    char** get_local_ipaddr(int& nSize);

    /* 删除本地ip地址数组 */
    void del_local_ipaddr(char** ppIpAddrs, int nSize);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
