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
#include "aios/apps/facility/cm2/cm_basic/leader_election/master_server.h"

#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "aios/apps/facility/cm2/cm_basic/common/zk_option.h"
#include "autil/StringUtil.h"

namespace cm_basic {

IMasterServer::IMasterServer() : _tcpPort(0) {}

IMasterServer::~IMasterServer() {}

int32_t IMasterServer::init() { return 0; }

void IMasterServer::setParameter(const std::string& zk_server, const std::string& zk_path, int64_t timeout_ms) {}

int32_t IMasterServer::open() { return 0; }

bool IMasterServer::parseServerSpec(const std::string& server_spec)
{
    std::vector<std::string> tokens;
    autil::StringUtil::split(tokens, server_spec, ':');
    if (tokens.size() < 2 || tokens.size() > 3) {
        return false;
    }
    _masterIp = tokens[0];
    if (!autil::StringUtil::strToInt32(tokens[1].c_str(), _tcpPort) || _tcpPort == 0) {
        return false;
    }
    if (tokens.size() == 3) {
        if (!autil::StringUtil::strToInt32(tokens[2].c_str(), _udpPort) || _udpPort == 0) {
            return false;
        }
    }
    return true;
}

AUTIL_LOG_SETUP(cm_basic, ZKMasterServer);

ZKMasterServer::ZKMasterServer(const std::string& zk_server, const std::string& zk_path, int64_t timeout)
    : IMasterServer()
    , _masterApply(NULL)
{
    _zkServer = zk_server;
    _zkPath = zk_path + "/" ZK_LE_PATH_NAME;
    _timeout = timeout;
}

int32_t ZKMasterServer::init()
{
    _masterApply = new (std::nothrow) MasterApply(_zkServer, _timeout);
    if (_masterApply == NULL)
        return -1;

    return 0;
}

void ZKMasterServer::setParameter(const std::string& zk_server, const std::string& zk_path, int64_t timeout_ms)
{
    _zkServer.assign(zk_server);
    _zkPath = zk_path + "/" ZK_LE_PATH_NAME;
    return _masterApply->setParameter(_zkServer, timeout_ms);
}

ZKMasterServer::~ZKMasterServer() { deletePtr(_masterApply); }

int32_t ZKMasterServer::open() { return 0; }

int32_t ZKMasterServer::getMaster()
{
    // server 从zk获取
    std::string spec;
    if (_masterApply->getMaster(_zkPath, spec) != 0 || spec.length() == 0) {
        // 获取master 失败，打一条log， 延用老的master
        AUTIL_LOG(ERROR, "ZKMasterServer::getMaster(), _zkPath = %s, spec = %s", _zkPath.c_str(), spec.c_str());
        return -1;
    }

    // 获取成功，使用新的cm master spec
    if (!parseServerSpec(spec)) {
        AUTIL_LOG(ERROR, "ZKMasterServer::parseServerSpec(), spec = %s", spec.c_str());
        return -1;
    }
    return 0;
}

AUTIL_LOG_SETUP(cm_basic, CfgMasterServer);

CfgMasterServer::CfgMasterServer(const char* cm_server) : IMasterServer() { _serverSpec = cm_server; }

CfgMasterServer::~CfgMasterServer() {}

int32_t CfgMasterServer::getMaster()
{
    if (!parseServerSpec(_serverSpec)) {
        AUTIL_LOG(ERROR, "hbnode : CfgMasterServer::parseServerSpec(), spec = %s", _serverSpec.c_str());
        return -1;
    }
    return 0;
}

} // namespace cm_basic
