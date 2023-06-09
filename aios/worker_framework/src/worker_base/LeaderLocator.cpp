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
#include "worker_framework/LeaderLocator.h"

#include <algorithm>
#include <vector>

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace autil;
using namespace std;
namespace worker_framework {
#define UNKNOWN_LEADER_ADDRESS "unknown"

AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, LeaderLocator);

LeaderLocator::LeaderLocator() { _zk = NULL; }

LeaderLocator::~LeaderLocator() {}

bool LeaderLocator::init(cm_basic::ZkWrapper *zk, const std::string &path, const std::string &baseName) {
    _zk = zk;
    _path = path;
    _baseName = baseName;
    return true;
}

string LeaderLocator::getLine(const string str, const uint n) {
    StringView cstr(str);
    vector<string> strs = StringTokenizer::tokenize(cstr, "\n", autil::StringTokenizer::TOKEN_TRIM);
    if (strs.size() < n) {
        return "";
    } else {
        return strs[n - 1];
    }
}

std::string LeaderLocator::getLeaderAddr() {
    string leaderAddr = doGetLeaderAddr();
    return getLine(leaderAddr, 1);
}

std::string LeaderLocator::getLeaderHTTPAddr() {
    string leaderAddr = doGetLeaderAddr();
    return getLine(leaderAddr, 2);
}

std::string LeaderLocator::doGetLeaderAddr() {
    assert(_zk);
    if (_zk->isBad()) {
        _zk->close();
        _zk->open();
    }
    std::vector<std::string> nodes;
    if (_zk->getChild(_path, nodes) != ZOK) {
        AUTIL_LOG(WARN, "LeaderLocator getChild failed. path:%s", _path.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    if (nodes.empty()) {
        AUTIL_LOG(DEBUG, "LeaderLocator no node failed. path:%s", _path.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    sort(nodes.begin(), nodes.end());
    std::string leaderNode = "";
    std::vector<std::string>::iterator it = nodes.begin();
    for (; it != nodes.end(); it++) {
        if (autil::StringUtil::startsWith(*it, _baseName)) {
            leaderNode = *it;
            break;
        }
    }
    if (leaderNode == "") {
        AUTIL_LOG(WARN, "no node start with baseName[%s].", _baseName.c_str());
    }

    std::string leaderAddr;
    if (_zk->getData(_path + '/' + leaderNode, leaderAddr) != ZOK) {
        AUTIL_LOG(WARN, "get leaderAddr from leaderNode[%s] failed.", leaderNode.c_str());
        return UNKNOWN_LEADER_ADDRESS;
    }
    return leaderAddr;
}

}; // namespace worker_framework
