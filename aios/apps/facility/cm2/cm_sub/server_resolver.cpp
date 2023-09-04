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
#include "aios/apps/facility/cm2/cm_sub/server_resolver.h"

#include "aios/apps/facility/cm2/cm_basic/common/zk_option.h"
#include "autil/StringUtil.h"

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, ZKServerResolver);

ZKServerResolver::ZKServerResolver(const std::string& zkhost, const std::string& zkroot, int timeout, int init_idx,
                                   bool sub_master)
    : _zkWrapper(zkhost, timeout)
    , _masterServer(NULL)
    , _zkPath(zkroot + "/" ZK_LE_PATH_NAME)
    , _idx(init_idx)
{
    if (sub_master) {
        _masterServer = new cm_basic::ZKMasterServer(zkhost.c_str(), zkroot.c_str(), timeout);
        _masterServer->init();
    }
}

ZKServerResolver::~ZKServerResolver()
{
    if (_masterServer != NULL) {
        delete _masterServer;
        _masterServer = NULL;
    }
}

bool ZKServerResolver::resolveMaster(std::string& ip, uint16_t& port)
{
    if (_masterServer->getMaster() != 0) {
        return false;
    }
    ip = _masterServer->_masterIp;
    port = _masterServer->_tcpPort;
    return true;
}

bool ZKServerResolver::resolve(std::string& ip, uint16_t& port)
{
    if (_masterServer != NULL) {
        return resolveMaster(ip, port);
    }
    if (!_zkWrapper.open()) {
        AUTIL_LOG(WARN, "zk wrapper open failed with status %d", _zkWrapper.getStatus());
        return false;
    }
    std::vector<std::string> vec_nodes;
    if (_zkWrapper.getChild(_zkPath, vec_nodes) != ZOK) {
        AUTIL_LOG(WARN, "get zk child failed on %s", _zkPath.c_str());
        _zkWrapper.close();
        return false;
    }
    if (vec_nodes.empty()) {
        AUTIL_LOG(WARN, "empty children on path %s", _zkPath.c_str());
        _zkWrapper.close();
        return false;
    }
    if (_idx < 0) { // set initial value
        _idx = (int)(rand() % vec_nodes.size());
    }
    _idx = (_idx + 1) % vec_nodes.size();

    std::string spec;
    std::string path = _zkPath + '/' + vec_nodes[_idx];
    if (ZOK != _zkWrapper.getData(path, spec)) {
        AUTIL_LOG(WARN, "get data on zk path failed: %s", path.c_str());
        _zkWrapper.close();
        return false;
    }
    if (!parseSpec(spec, ip, port)) {
        AUTIL_LOG(WARN, "parse spec failed: %s", spec.c_str());
        _zkWrapper.close();
        return false;
    }
    _zkWrapper.close();
    return true;
}

bool IServerResolver::parseSpec(const std::string& spec, std::string& ip, uint16_t& port)
{
    std::vector<std::string> secs = autil::StringUtil::split(spec, ":");
    if (secs.size() < 2)
        return false;
    ip = secs[0];
    return autil::StringUtil::strToUInt16(secs[1].c_str(), port);
}

} // namespace cm_sub
