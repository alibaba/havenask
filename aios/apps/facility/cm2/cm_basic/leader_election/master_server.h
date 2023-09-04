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
 *       Filename:  master_server.h
 *
 *    Description:  提供基础函数getMaster, 从zk上获取主的cm_server
 *
 *        Version:  0.1
 *        Created:  2013-03-17 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __MASTER_SERVER_H_
#define __MASTER_SERVER_H_

#include <stdint.h>
#include <stdio.h>

#include "aios/apps/facility/cm2/cm_basic/leader_election/master_apply.h"
#include "autil/Log.h"

namespace cm_basic {

class IMasterServer
{
public:
    std::string _masterIp;
    int32_t _tcpPort;
    int32_t _udpPort;

protected:
    /*
     * @brief   解析cm master spec
     * @param   [in]    server_spec=10.232.42.113:6677:7788
     * @return  true:  succuss,    false: failed
     */
    bool parseServerSpec(const std::string& server_spec);

public:
    IMasterServer();
    virtual ~IMasterServer();
    virtual int32_t init();
    virtual void setParameter(const std::string& zk_server, const std::string& zk_path, int64_t timeout_ms = 10000);
    virtual int32_t open();
    virtual int32_t getMaster() = 0;
};

class ZKMasterServer : public IMasterServer
{
private:
    cm_basic::MasterApply* _masterApply;
    std::string _zkServer;
    std::string _zkPath;
    int64_t _timeout; // zk超时时间，单位(秒)

public:
    ZKMasterServer(const std::string& zk_server, const std::string& zk_path, int64_t timeout = 10);
    ~ZKMasterServer();
    void setParameter(const std::string& zk_server, const std::string& zk_path, int64_t timeout_ms = 10000);
    int32_t init();
    int32_t open();
    virtual int32_t getMaster();

private:
    AUTIL_LOG_DECLARE();
};

class CfgMasterServer : public IMasterServer
{
private:
    std::string _serverSpec;

public:
    CfgMasterServer(const char* cm_server);
    ~CfgMasterServer();
    virtual int32_t getMaster();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
