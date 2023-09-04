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
#include "sdk/AmLeaderChecker.h"
#include "util/PathUtil.h"

using namespace std;
using namespace worker_framework;

BEGIN_HIPPO_NAMESPACE(sdk);
USE_HIPPO_NAMESPACE(util);

HIPPO_LOG_SETUP(sdk, AmLeaderChecker);

const string AmLeaderChecker::LEADER_ELECTION_BASE_FILE = "leader_election0000000000";

AmLeaderChecker::AmLeaderChecker(LeaderChecker * leaderChecker) {
    _zkWrapper = NULL;
    _leaderChecker = leaderChecker;
    _isOwner = false;
}

AmLeaderChecker::~AmLeaderChecker() {
    if (_isOwner) {
        delete _leaderChecker;
    }
    delete _zkWrapper;
}

bool AmLeaderChecker::createPrivateLeaderChecker(
        const string &leaderElectRoot, int64_t sessionTimeout,
        const string &baseName, const string &myAddr)
{
    string zkHost = PathUtil::getHostFromZkPath(leaderElectRoot);
    if (zkHost.empty()) {
        HIPPO_LOG(ERROR, "invalid leader elect root:[%s]",
                  leaderElectRoot.c_str());
        return false;
    }
    _zkWrapper = new cm_basic::ZkWrapper(zkHost,
            sessionTimeout / 1000000);

    string leaderLockPath = PathUtil::getPathFromZkPath(leaderElectRoot) + "LeaderElectionLock";
    _leaderChecker = new LeaderChecker(_zkWrapper, leaderLockPath);

    string leaderInfoPath = PathUtil::joinPath(
            PathUtil::getPathFromZkPath(leaderElectRoot), baseName + "0000000000");
    if (baseName != "leader_election") {
        bool exist;
        string oldLeaderInfoPath = PathUtil::joinPath(
                PathUtil::getPathFromZkPath(leaderElectRoot), "leader_election0000000000");
        if (!_zkWrapper->open()) {
            HIPPO_LOG(ERROR, "failed to connect zk");
            return false;
        }
        if (!_zkWrapper->check(oldLeaderInfoPath, exist)) {
            HIPPO_LOG(ERROR, "failed to check path[%s] exist", oldLeaderInfoPath.c_str());
            return false;
        }
        if (exist && !_zkWrapper->remove(oldLeaderInfoPath)) {
            HIPPO_LOG(ERROR, "failed to remove path[%s] exist", oldLeaderInfoPath.c_str());
            return false;
        }
    }
    _leaderChecker->setLeaderInfoPath(leaderInfoPath);
    _leaderChecker->setLeaderInfo(myAddr);

    _isOwner = true;
    return true;
}

bool AmLeaderChecker::isLeader() {
    if (!_leaderChecker) {
        return false;
    }
    if (_isOwner) {
        _leaderChecker->tryLock();
    }
    return _leaderChecker->isLeader();
}

END_HIPPO_NAMESPACE(sdk);
