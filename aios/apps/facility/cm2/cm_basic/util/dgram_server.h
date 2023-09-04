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
 *       Filename:  dgram_server.h
 *
 *    Description:  ClusterMap库的功能类，UDP心跳服务端
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

#ifndef __CM_BASIC_DGRAM_SERVER_H__
#define __CM_BASIC_DGRAM_SERVER_H__

#include <sys/socket.h>

#include "aios/apps/facility/cm2/cm_basic/util/dgram_common.h"

namespace cm_basic {

class CDGramServer
{
public:
    /* 构造函数,配置参数 */
    CDGramServer();

    /* 析构函数 */
    virtual ~CDGramServer(void);

    int32_t InitDGramServer(int port);

    /* 接收消息，返回的是消息内容及长度(pMsg, nLen)，同时返回主机名和端口 */
    int32_t receive(char* buf, int buf_size);

    /* 获得DgramServer用的socked标识 */
    inline int getSocked() { return m_sock; }

private:
    int m_sock;                // use this socket to receive
    CDGramCommon m_cDGramComm; // udp的公共类
};

} // namespace cm_basic
#endif
