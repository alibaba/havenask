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
 *    Description:  ClusterMap库的功能类，UDP心跳客户端
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

#ifndef __CM_BASIC_DGRAM_CLIENT_H__
#define __CM_BASIC_DGRAM_CLIENT_H__

#include <netinet/in.h>
#include <sys/socket.h>

#include "aios/apps/facility/cm2/cm_basic/util/dgram_common.h"

namespace cm_basic {

class CDGramClient
{
public:
    /* 构造函数,配置参数 */
    CDGramClient();

    /* 析构函数 */
    virtual ~CDGramClient(void);

    int32_t InitDGramClient(const char* pIp, uint16_t nPort);

    /* 发送消息，输入参数是消息指针和长度 */
    int send(const char* pMsg, int pLen);

private:
    int m_sock;                // use this socket to send
    sockaddr_in m_saddr;       // server's address
    CDGramCommon m_cDGramComm; // udp的公共类
};

} // namespace cm_basic
#endif
