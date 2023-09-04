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
#include "worker_framework/LeaderedWorkerBase.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "worker_framework/PathUtil.h"

using namespace std;
using namespace autil;
using namespace worker_framework;

namespace worker_framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, LeaderedWorkerBase);

class LeaderedWorkerBaseImpl {
public:
    LeaderedWorkerBaseImpl() : zkWrapper(NULL), leaderElector(NULL), forceExitTs(-1) {}
    ~LeaderedWorkerBaseImpl() {
        DELETE_AND_SET_NULL(leaderElector);
        DELETE_AND_SET_NULL(zkWrapper);
    }

public:
    cm_basic::ZkWrapper *zkWrapper;
    LeaderElector *leaderElector;
    string zkPath;
    int64_t forceExitTs;
};

LeaderedWorkerBase::LeaderedWorkerBase() : _impl(new LeaderedWorkerBaseImpl) {}

LeaderedWorkerBase::~LeaderedWorkerBase() {
    shutdownLeaderElector();
    DELETE_AND_SET_NULL(_impl);
}

bool LeaderedWorkerBase::initLeaderElector(const string &zkRoot,
                                           int64_t leaseInSec,
                                           int64_t loopInterval,
                                           const string &progressKey) {
    if (initLeaderElectorNotStart(zkRoot, leaseInSec * 1000, loopInterval * 1000, progressKey)) {
        return startLeaderElector();
    }
    return false;
}

bool LeaderedWorkerBase::initLeaderElectorNotStart(const string &zkRoot,
                                                   int64_t leaseInMs,
                                                   int64_t loopIntervalMs,
                                                   const string &progressKey) {
    const string &zkHost = PathUtil::getHostFromZkPath(zkRoot);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zkRoot:[%s]", zkRoot.c_str());
        return false;
    }
    _impl->zkWrapper = new cm_basic::ZkWrapper(zkHost, DEFAULT_ZK_TIMEOUT);
    _impl->zkPath = PathUtil::getPathFromZkPath(zkRoot);
    // leader info path and leader lock path must be different
    string currentPath;
    if (!PathUtil::getCurrentPath(currentPath)) {
        AUTIL_LOG(ERROR, "getCurrentPath fail[%s]", currentPath.c_str());
        return false;
    }
    _impl->leaderElector = new LeaderElector(
        _impl->zkWrapper, getLeaderElectorLockPath(), leaseInMs, loopIntervalMs, true, progressKey, currentPath);
    _impl->leaderElector->setLeaderInfoPath(getLeaderInfoPath());
    _impl->leaderElector->setLeaderInfo(getLeaderInfo());

    getLeaderElector()->setBecomeLeaderHandler(std::bind(&LeaderedWorkerBase::becomeLeader, this));
    getLeaderElector()->setNoLongerLeaderHandler(std::bind(&LeaderedWorkerBase::noLongerLeader, this));
    getLeaderElector()->setPreemptedHandler(std::bind(&LeaderedWorkerBase::preempted, this));
    return true;
}

bool LeaderedWorkerBase::initLeaderElector(const string &zkRoot) {
    return initLeaderElector(zkRoot, LEASE_IN_SECOND, LOOP_INTERVAL, "");
}

bool LeaderedWorkerBase::isLeader() const { return _impl->leaderElector->isLeader(); }

bool LeaderedWorkerBase::waitToBeLeader(uint32_t retryTimes, uint32_t sleepEachRetry) const {
    assert(_impl->leaderElector);
    for (size_t i = 0; i < retryTimes; i++) {
        if (isLeader()) {
            return true;
        }
        usleep(sleepEachRetry);
    }
    AUTIL_LOG(ERROR, "wait to be leader failed");
    return false;
}

bool LeaderedWorkerBase::needSlotIdInLeaderInfo() const { return false; }
int32_t LeaderedWorkerBase::getSlotId() const {
    static string HIPPO_ENV_SLOT_ID = "HIPPO_SLOT_ID";
    return autil::StringUtil::numberFromString<int32_t>(autil::EnvUtil::getEnv(HIPPO_ENV_SLOT_ID));
}

string LeaderedWorkerBase::getLeaderInfo() const {
    vector<string> infos;
    infos.push_back(getAddress());
    if (supportHTTP()) {
        infos.resize(2);
        infos[1] = getHTTPAddress();
    }
    if (needSlotIdInLeaderInfo()) {
        infos.resize(3);
        infos[2] = std::to_string(getSlotId());
    }
    return autil::StringUtil::join(infos, "\n");
}

string LeaderedWorkerBase::getLeaderElectorPath() const { return PathUtil::getLeaderInfoDir(_impl->zkPath); }

string LeaderedWorkerBase::getLeaderElectorLockPath() const {
    return PathUtil::joinPath(_impl->zkPath, "LeaderElectionLock");
}

string LeaderedWorkerBase::getLeaderInfoPath() const { return PathUtil::getLeaderInfoFile(getLeaderElectorPath()); }

LeaderElector::LeaderState LeaderedWorkerBase::getLeaderState() const { return _impl->leaderElector->getLeaderState(); }

cm_basic::ZkWrapper *LeaderedWorkerBase::getZkWrapper() const { return _impl->zkWrapper; }

LeaderElector *LeaderedWorkerBase::getLeaderElector() const { return _impl->leaderElector; }

void LeaderedWorkerBase::preempted() {
    int64_t currentTs = autil::TimeUtility::currentTimeInSeconds();
    if (_impl->forceExitTs == -1) {
        int64_t waitTsInSecond = 120, tmpTs = -1;
        string value = autil::EnvUtil::getEnv("WF_PREEMPTED_WAIT_TS");
        if (!value.empty()) {
            if (StringUtil::fromString(value, tmpTs) && tmpTs > 0) {
                waitTsInSecond = tmpTs;
            }
        }

        _impl->forceExitTs = currentTs + waitTsInSecond;
        AUTIL_LOG(INFO, "preempted, will force exit at [%ld]", _impl->forceExitTs);
        // default wait 120 second, maybe already signal 10
    } else if (currentTs > _impl->forceExitTs) {
        AUTIL_LOG(WARN, "preempted timeout, force exit!");
        _exit(0);
    } else {
        AUTIL_LOG(INFO, "waiting exit");
    }
}

bool LeaderedWorkerBase::startLeaderElector() {
    if (_impl->leaderElector) {
        return _impl->leaderElector->start();
    }
    return false;
}

void LeaderedWorkerBase::shutdownLeaderElector() {
    if (_impl->leaderElector) {
        _impl->leaderElector->stop();
        DELETE_AND_SET_NULL(_impl->leaderElector);
    }
}

}; // namespace worker_framework
