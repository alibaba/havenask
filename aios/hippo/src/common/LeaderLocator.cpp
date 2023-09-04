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
#include "common/LeaderLocator.h"
#include <algorithm>
#include <functional>
#include <functional>
#include "autil/StringUtil.h"

using namespace std;
using namespace std::placeholders;
BEGIN_HIPPO_NAMESPACE(common);

HIPPO_LOG_SETUP(common, LeaderLocator);

LeaderLocator::LeaderLocator() {
    _zk = NULL;
}

LeaderLocator::~LeaderLocator() {
}

bool LeaderLocator::init(cm_basic::ZkWrapper * zk,
                         const std::string &path, const std::string &baseName)
{
    _zk = zk;
    _path = path;
    _baseName = baseName;
    return true;
}

std::string LeaderLocator::getLeaderAddr() {
    assert(_zk);
    if (_zk->isBad()) {
        _zk->close();
        _zk->open();
    }
    return doGetLeaderAddr();
}

std::string LeaderLocator::doGetLeaderAddr() {
    assert(_zk);
    std::vector<std::string> nodes;
    if (_zk->getChild(_path, nodes) != ZOK) {
        HIPPO_LOG(WARN, "LeaderLocator getChild failed. path:%s", _path.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    if (nodes.empty()) {
        HIPPO_LOG(WARN, "LeaderLocator no node failed. path:%s", _path.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    sort(nodes.begin(), nodes.end());
    std::string leaderNode = "";
    std::vector<std::string>::iterator it= nodes.begin();
    for (; it != nodes.end(); it++) {
        if (autil::StringUtil::startsWith(*it, _baseName)) {
            leaderNode = *it;
            break;
        }
    }
    if (leaderNode == "") {
        HIPPO_LOG(WARN, "no node start with baseName[%s].", _baseName.c_str());
    }

    std::string leaderAddr;
    if (_zk->getData(_path + '/' + leaderNode, leaderAddr) != ZOK) {
        HIPPO_LOG(WARN, "get leaderAddr from leaderNode[%s] failed.",
                  leaderNode.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    return leaderAddr;
}

END_HIPPO_NAMESPACE(common);
