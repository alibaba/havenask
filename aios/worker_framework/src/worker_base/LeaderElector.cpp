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
#include "worker_framework/LeaderElector.h"

#include <signal.h>
#include <sys/types.h>
#include <utime.h>

#include "autil/Log.h"
#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "worker_framework/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

namespace worker_framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.worker_base, LeaderElector);

const string LeaderElector::LEADER_LEASE_FILE_NAME = "leader_lease";
const string LeaderElector::LEADER_VERSION_FILE_NAME = "leader_version";
const string LeaderElector::LEADER_LEASE_FILE_SPLIT_SIGN = "#_#";
const string LeaderElector::LOCAL_LOCK_FILE_SPLIT_SIGN = "#_#";
const string LeaderElector::DEFAULT_PROGRESS_KEY = "";
const string LeaderElector::WF_PREEMPTIVE_LEADER = "WF_PREEMPTIVE_LEADER";

LeaderElector::LeaderElector(cm_basic::ZkWrapper *zkWrapper,
                             const string &leaderPath,
                             int64_t leaseInSec,
                             int64_t loopIntervalInSec,
                             const string &progressKey,
                             const string &workDir)
                             : LeaderElector(zkWrapper, leaderPath, leaseInSec * 1000,
                               loopIntervalInSec * 1000, true, progressKey, workDir)
{
}

LeaderElector::LeaderElector(cm_basic::ZkWrapper *zkWrapper,
                             const string &leaderPath,
                             int64_t leaseInMs,
                             int64_t loopIntervalInMs,
                             bool millisecond,
                             const string &progressKey,
                             const string &workDir)
    : _leaderInfoPath(PathUtil::getLeaderInfoFile(PathUtil::getLeaderInfoDir(leaderPath)))
    , _zkWrapper(zkWrapper)
    , _leaderPath(leaderPath)
    , _leaseTimeoutUs(leaseInMs * 1000)
    , _loopIntervalUs(loopIntervalInMs * 1000)
    , _localLockVersion(-1)
    , _progressKey(progressKey)
    , _workDir(workDir)
    , _isPreempted(false) {
    const char *value = getenv(WF_PREEMPTIVE_LEADER.c_str());
    if (value) {
        bool enable = false;
        autil::StringUtil::parseTrueFalse(value, enable);
        if (enable) {
            _localLockVersionFile = fs::FileSystem::joinFilePath(
                workDir,
                string(".") +
                    StringUtil::toString(MurmurHash::MurmurHash64A(leaderPath.c_str(), leaderPath.length(), 0)));
        }
    }
    AUTIL_LOG(INFO, "init LeaderElector, leaderPath = [%s], leaseTimeoutUs = [%ld], loopIntervalUs = [%ld]",
            _leaderInfoPath.c_str(), _leaseTimeoutUs, _loopIntervalUs);
}

LeaderElector::~LeaderElector() { stop(); }

bool LeaderElector::start() {
    if (!readValidLocalLockFile(_localLockVersionFile, _localLockVersion)) {
        return false;
    }
    _leaderLockThreadPtr =
        LoopThread::createLoopThread(bind(&LeaderElector::tryLock, this), _loopIntervalUs, "WfLeaderLock");
    _leaderCheckThreadPtr =
        LoopThread::createLoopThread(bind(&LeaderElector::checkLeader, this), _loopIntervalUs, "WfLeaderCheck");
    if (!_leaderLockThreadPtr || !_leaderCheckThreadPtr) {
        AUTIL_LOG(ERROR, "create leader check thread fail");
        return false;
    }
    AUTIL_LOG(INFO, "[%p] start leader elector.", this);
    return true;
}

void LeaderElector::stop() {
    if (!_leaderLockThreadPtr && !_leaderCheckThreadPtr) {
        return;
    }

    _leaderLockThreadPtr.reset();
    _leaderCheckThreadPtr.reset();
    unlock();
    if (!_localLockVersionFile.empty()) {
        // for PREEMPTIVE leader election strategy, forbidden old worker becoming leader
        // restart interval time longer, to let AM release slot
        usleep(5 * 1000 * 1000);
    }
    AUTIL_LOG(INFO, "[%p] stop leader elector.", this);
}

bool LeaderElector::tryLock() {
    int64_t currentTime = TimeUtility::currentTime();
    workLoop(currentTime);
    return isLeader();
}

void LeaderElector::workLoop(int64_t currentTime) {
    if (currentTime < _nextCanCampaignTime.load()) {
        return;
    }
    AUTIL_LOG(DEBUG, "begin workloop, current time[%ld]", currentTime);
    int64_t beginTime = TimeUtility::monotonicTimeUs();
    if (beginTime - _lastLoopTime < 0 || (beginTime - _lastLoopTime > _loopIntervalUs * 2)) {
        AUTIL_LOG(WARN, "begin workloop timeout, current time[%ld] last time[%ld]", beginTime, _lastLoopTime);
    }
    _lastLoopTime = beginTime;
    if (!_zkWrapper->isConnected()) {
        if (!_zkWrapper->open()) {
            AUTIL_LOG(ERROR, "connect to zookeeper failed");
            // check is still leader?
            checkStillLeader(currentTime);
            return;
        }
    }

    if (!handleVersion()) {
        AUTIL_LOG(INFO, "handle version failed");
        return;
    }
    int64_t handleVersionTime = TimeUtility::monotonicTimeUs();
    if (!isLeader()) {
        campaignLeader(currentTime);
    } else {
        holdLeader(currentTime);
    }
    int64_t endTime = TimeUtility::monotonicTimeUs();
    if (endTime - beginTime > _leaseTimeoutUs / 2) {
        AUTIL_LOG(WARN,
                  "workloop run timeout, begin time[%ld], handle time[%ld] end time[%ld]",
                  beginTime,
                  handleVersionTime,
                  endTime);
    }
}

void LeaderElector::campaignLeader(int64_t currentTime) {
    if (currentTime < _nextCanCampaignTime.load()) {
        return;
    }
    int64_t leaderLeaseExpirationTime;
    string progressKey;
    if (!getLeaderLeaseExpirationTime(leaderLeaseExpirationTime, progressKey)) {
        // read file failed.
        return;
    }
    if (currentTime <= leaderLeaseExpirationTime && !progressKeyEqual(progressKey)) {
        AUTIL_LOG(DEBUG,
                  "can't campaign for leader now, "
                  "last leader time to [%ld], current time[%ld]",
                  leaderLeaseExpirationTime,
                  currentTime);
        return;
    }
    if (lockForLease()) {
        if (doCampaign(currentTime)) {
            unlockForLease();
            AUTIL_LOG(INFO, "become leader success!");
            if (_becomeLeaderHandler) {
                _becomeLeaderHandler();
            }
        } else {
            unlockForLease();
        }
        // if unlock fail, zk EPHEMERAL node may be disappear
    } else {
        AUTIL_LOG(WARN, "lock for zookeeper failed, zk will close");
        _zkWrapper->close();
    }
}

bool LeaderElector::doCampaign(int64_t currentTime) {
    int64_t leaderLeaseExpirationTime;
    string progressKey;
    // double check in lock
    if (!getLeaderLeaseExpirationTime(leaderLeaseExpirationTime, progressKey)) {
        // read file failed.
        return false;
    }
    bool keyEqual = progressKeyEqual(progressKey);
    if (currentTime <= leaderLeaseExpirationTime && !keyEqual) {
        AUTIL_LOG(DEBUG,
                  "can't campaign leader temporary, lease time [%ld], current time [%ld]",
                  leaderLeaseExpirationTime,
                  currentTime);
        return false;
    }

    AUTIL_LOG(INFO,
              "already can campaign for leader now, "
              "last leader time to [%ld], current time[%ld]",
              leaderLeaseExpirationTime,
              currentTime);
    int64_t leaseExpirationTime = currentTime + _leaseTimeoutUs;
    if (!reletLeaderTime(leaseExpirationTime)) {
        return false;
    }
    updateLeaderInfo(true, leaseExpirationTime);
    // handle become leader
    return true;
}

void LeaderElector::holdLeader(int64_t currentTime) {
    if (!checkStillLeader(currentTime)) {
        return;
    }
    if (lockForLease()) {
        AUTIL_LOG(DEBUG, "get lock success");
        doHoldLeader(currentTime);
        unlockForLease();
        AUTIL_LOG(DEBUG, "unlock success");
    } else {
        AUTIL_LOG(WARN, "lock for zookeeper failed, zk will close");
        _zkWrapper->close();
    }
}

void LeaderElector::doHoldLeader(int64_t currentTime) {
    int64_t leaseExpirationTime = currentTime + _leaseTimeoutUs;
    if (!reletLeaderTime(leaseExpirationTime)) {
        return;
    }
    updateLeaderInfo(true, leaseExpirationTime);
}

void LeaderElector::checkLeader() {
    int64_t currentTime = TimeUtility::currentTime();
    checkStillLeader(currentTime);
}

bool LeaderElector::checkStillLeader(int64_t currentTime) {
    if (!isLeader()) {
        return false;
    }
    // leave some spare time to prevent leader conflict
    int64_t leaseExpirationTime = getLeaseExpirationTime();
    if (leaseExpirationTime - currentTime > _leaseTimeoutUs / 2) {
        return true;
    } else {
        // handle no longer leader
        if (_forbidCampaignTimeMs > 0) {
            _nextCanCampaignTime.store(currentTime + _forbidCampaignTimeMs * 1000);
        }
        AUTIL_LOG(WARN,
                  "no longer leader, lease expiration time [%ld], current time [%ld], next campiagn time [%ld]",
                  leaseExpirationTime,
                  currentTime, _nextCanCampaignTime.load());
        updateLeaderInfo(false, -1);
        if (_noLongerLeaderHandler) {
            _noLongerLeaderHandler();
        }

        return false;
    }
}

bool LeaderElector::getLeaderLeaseExpirationTime(int64_t &lastLeaderEndTime, string &progressKey) {
    bool isExist;
    string leaseTimeFile = getLeaseTimeFile(_leaderPath);
    if (!_zkWrapper->check(leaseTimeFile, isExist)) {
        AUTIL_LOG(ERROR, "check leader file exist for [%s] failed", leaseTimeFile.c_str());
        return false;
    }
    if (!isExist) {
        lastLeaderEndTime = -1;
        return true;
    }
    string leaderFileContentStr;
    if (_zkWrapper->getData(leaseTimeFile, leaderFileContentStr) != ZOK) {
        AUTIL_LOG(ERROR, "get data for zk path [%s] failed", leaseTimeFile.c_str());
        return false;
    }
    if (!getLeaderEndTimeFromString(leaderFileContentStr, lastLeaderEndTime, progressKey)) {
        AUTIL_LOG(ERROR, "get leader end time for path [%s] failed", leaseTimeFile.c_str());
        return false;
    }
    return true;
}

bool LeaderElector::progressKeyEqual(const string &progressKey) {
    if (_progressKey == DEFAULT_PROGRESS_KEY) {
        return false;
    }
    if (_progressKey != progressKey) {
        AUTIL_LOG(
            DEBUG, "progress key not equal to zk, your key[%s], zk key[%s]", _progressKey.c_str(), progressKey.c_str());
        return false;
    }
    return true;
}

bool LeaderElector::getLeaderEndTimeFromString(const string &leaderFileContentStr,
                                               int64_t &lastLeaderEndTime,
                                               string &progressKey) {
    auto lines = StringUtil::split(leaderFileContentStr, "\n");
    if (lines.empty()) {
        AUTIL_LOG(ERROR, "leader lease time file not right! leader time [%s]", leaderFileContentStr.c_str());
        return false;
    }
    vector<string> leaseFileParamVec = StringUtil::split(lines[0], LEADER_LEASE_FILE_SPLIT_SIGN);
    if (leaseFileParamVec.size() == 0 || leaseFileParamVec.size() > 2) {
        AUTIL_LOG(ERROR, "leader lease time file not right! leader time [%s]", leaderFileContentStr.c_str());
        return false;
    }
    string lastLeaderEndTimeStr = leaseFileParamVec[0];
    progressKey = DEFAULT_PROGRESS_KEY;
    if (leaseFileParamVec.size() == 2) {
        progressKey = leaseFileParamVec[1];
    }
    if (!StringUtil::fromString(lastLeaderEndTimeStr, lastLeaderEndTime)) {
        AUTIL_LOG(ERROR, "leader time[%s] is not a number", lastLeaderEndTimeStr.c_str());
        return false;
    }
    return true;
}

bool LeaderElector::lockForLease() {
    bool isExist;
    if (!_zkWrapper->check(_leaderPath, isExist)) {
        return false;
    }
    if (!isExist && !_zkWrapper->createPath(_leaderPath)) {
        AUTIL_LOG(WARN, "create leader path[%s] failed", _leaderPath.c_str());
        return false;
    }
    string lockPath = getLockPath(_leaderPath);
    if (!_zkWrapper->createNode(lockPath, "")) {
        AUTIL_LOG(INFO, "lock file [%s] failed", lockPath.c_str());
        return false;
    }
    AUTIL_LOG(DEBUG, "create lock file [%s] success", lockPath.c_str());
    return true;
}

bool LeaderElector::unlockForLease() {
    string lockPath = getLockPath(_leaderPath);
    if (!_zkWrapper->remove(lockPath)) {
        AUTIL_LOG(WARN, "delete zk node [%s] failed, maybe zk is disconnect", lockPath.c_str());
        return false;
    }
    AUTIL_LOG(DEBUG, "release lock file [%s] success", lockPath.c_str());
    return true;
}

bool LeaderElector::reletLeaderTime(int64_t leaseExpirationTime) {
    string leaderLeaseFileStr = generateLeaderLeaseFileStr(leaseExpirationTime);
    string leaseTimeFile = getLeaseTimeFile(_leaderPath);

    if (!writeForZk(leaseTimeFile, leaderLeaseFileStr, true)) {
        AUTIL_LOG(ERROR,
                  "fail to set leaderLeaseFileStr[%s] to zk path [%s]",
                  leaderLeaseFileStr.c_str(),
                  leaseTimeFile.c_str());
        return false;
    }
    AUTIL_LOG(DEBUG, "relet leader time [%s].", leaderLeaseFileStr.c_str());
    return true;
}

string LeaderElector::generateLeaderLeaseFileStr(int64_t leaseExpirationTime) {
    std::string leaseInfo;
    if (_progressKey == DEFAULT_PROGRESS_KEY) {
        leaseInfo = StringUtil::toString(leaseExpirationTime);
    } else {
        leaseInfo = StringUtil::toString(leaseExpirationTime) + LEADER_LEASE_FILE_SPLIT_SIGN + _progressKey;
    }
    ScopedLock lock(_leaderInfoMutex);
    if (!_leaseInfo.empty()) {
        leaseInfo += "\n" + _leaseInfo;
    }
    return leaseInfo;
}

void LeaderElector::unlock() {
    if (!isLeader()) {
        AUTIL_LOG(INFO, "not leader, do not release lease");
        return;
    }
    // don't call no longer leader
    updateLeaderInfo(false, -1);
    if (!lockForLease()) {
        AUTIL_LOG(WARN, "lock file failed when stop, can't remove lease time file");
        _zkWrapper->close();
        return;
    }
    string filePath = getLeaderVersionPath(_leaderPath);
    vector<string> child;
    if (_localLockVersion != -1) {
        auto ec = _zkWrapper->getChild(filePath, child);
        if (ec == ZOK) {
            for (auto &single : child) {
                int64_t version;
                if (!StringUtil::fromString(single, version)) {
                    continue;
                }
                if (version < _localLockVersion) {
                    if (_zkWrapper->remove(fs::FileSystem::joinFilePath(filePath, single))) {
                        AUTIL_LOG(INFO, "remove remote lock version [%ld]", version);
                    }
                }
            }
        }
    }
    string leaseTimeFile = getLeaseTimeFile(_leaderPath);
    if (!_zkWrapper->remove(leaseTimeFile)) {
        AUTIL_LOG(WARN,
                  "delete zk node [%s] failed, maybe zk is disconnect,"
                  " can't remove lease time file",
                  leaseTimeFile.c_str());
    }
    string leaderInfoPath = getLeaderInfoPath();
    if (!leaderInfoPath.empty() && !_zkWrapper->remove(leaderInfoPath)) {
        AUTIL_LOG(WARN,
                  "delete zk node [%s] failed, maybe zk is disconnect,"
                  " can't remove leader info file",
                  leaderInfoPath.c_str());
    }
    unlockForLease();
    if (!_isPreempted && !_localLockVersionFile.empty()) {
        // gracefully unlock will remove local lock file
        if (fs::FileSystem::removeFile(_localLockVersionFile) != EC_OK) {
            AUTIL_LOG(WARN, "remove [%s] failed", _localLockVersionFile.c_str());
        }
    }
    AUTIL_LOG(INFO, "release lease success");
}

void LeaderElector::setNoLongerLeaderHandler(HandlerFuncType handler) { _noLongerLeaderHandler = handler; }

void LeaderElector::setBecomeLeaderHandler(HandlerFuncType handler) { _becomeLeaderHandler = handler; }

void LeaderElector::setPreemptedHandler(HandlerFuncType handler) { _preemptedHandler = handler; }
void LeaderElector::setForbidCampaignLeaderTimeMs(uint64_t forbitTime) {
    _forbidCampaignTimeMs = forbitTime;
}
void LeaderElector::updateLeaderInfo(bool toBeLeader, int64_t leaseExpirationTime) {
    // set leader info only when become leader
    if (!isLeader() && toBeLeader) {
        string leaderInfoPath;
        string leaderInfo;
        {
            ScopedLock lock(_leaderInfoMutex);
            leaderInfoPath = _leaderInfoPath;
            leaderInfo = _leaderInfo;
        }
        // write leader info failed, retry
        if (!writeLeaderInfo(leaderInfoPath, leaderInfo)) {
            return;
        }
    }
    {
        ScopedLock lock(_mutex);
        _state.isLeader = toBeLeader;
        _state.leaseExpirationTime = leaseExpirationTime;
    }
    AUTIL_LOG(DEBUG, "update lease expiration time [%ld]", _state.leaseExpirationTime);
}

bool LeaderElector::isLeader() const {
    ScopedLock lock(_mutex);
    return _state.isLeader;
}

// before this time, we always be leader
int64_t LeaderElector::getLeaseExpirationTime() const {
    ScopedLock lock(_mutex);
    return _state.leaseExpirationTime;
}

string LeaderElector::getLockPath(const std::string leaderPath) {
    return FileSystem::joinFilePath(leaderPath, "__lock__");
}

string LeaderElector::getLeaseTimeFile(const std::string leaderPath) {
    return FileSystem::joinFilePath(leaderPath, LEADER_LEASE_FILE_NAME);
}

string LeaderElector::getLeaderVersionPath(const std::string leaderPath) {
    return FileSystem::joinFilePath(leaderPath, LEADER_VERSION_FILE_NAME);
}

void LeaderElector::setLeaseInfo(const std::string &leaseInfo) {
    ScopedLock lock(_leaderInfoMutex);
    _leaseInfo = leaseInfo;
}

void LeaderElector::setLeaderInfo(const std::string &leaderInfo) {
    ScopedLock lock(_leaderInfoMutex);
    _leaderInfo = leaderInfo;
}

void LeaderElector::setLeaderInfoPath(const std::string &leaderInfoPath) {
    ScopedLock lock(_leaderInfoMutex);
    _leaderInfoPath = leaderInfoPath;
}

std::string LeaderElector::getLeaderInfo() const {
    ScopedLock lock(_leaderInfoMutex);
    return _leaderInfo;
}

std::string LeaderElector::getLeaderInfoPath() const {
    ScopedLock lock(_leaderInfoMutex);
    return _leaderInfoPath;
}

bool LeaderElector::writeLeaderInfo(const std::string &leaderInfoPath, const std::string &leaderInfo) {
    if (leaderInfoPath.empty() || leaderInfo.empty()) {
        AUTIL_LOG(DEBUG,
                  "some param is empty, ignore. leader info path[%s], leader info[%s]",
                  leaderInfoPath.c_str(),
                  leaderInfo.c_str());
        return true;
    }
    AUTIL_LOG(INFO, "write leader info[%s] for leader path[%s] ", leaderInfo.c_str(), leaderInfoPath.c_str());
    if (!writeForZk(leaderInfoPath, leaderInfo, true)) {
        AUTIL_LOG(
            ERROR, "write leader info[%s] for leader path[%s] failed", leaderInfo.c_str(), leaderInfoPath.c_str());
        return false;
    }
    return true;
}

bool LeaderElector::writeForZk(const string &zkPath, const string &content, bool permanent) {
    bool isExist = false;
    if (!_zkWrapper->check(zkPath, isExist, false)) {
        AUTIL_LOG(ERROR, "check stateFile[%s] isExist failed.", zkPath.c_str());
        return false;
    }
    if (!isExist) {
        return _zkWrapper->createNode(zkPath, content, permanent);
    }
    return _zkWrapper->set(zkPath, content);
}

bool LeaderElector::readValidLocalLockFile(const string &localFilePath, int64_t &version) {
    if (_localLockVersionFile.empty()) {
        return true;
    }
    version = -1;
    FileMeta fileMeta;
    auto ec = fs::FileSystem::getFileMeta(localFilePath, fileMeta);
    if (ec == EC_NOENT) {
        return true;
    }

    if (ec != EC_OK) {
        AUTIL_LOG(ERROR, "get meta [%s] failed", localFilePath.c_str());
        return false;
    }
    auto currentTs = TimeUtility::currentTimeInSeconds();
    int64_t leaseInMin = 30, tmpMin = -1;
    const char *value = getenv("WF_LOCAL_LOCK_LEASE_IN_MIN");
    if (value) {
        if (StringUtil::fromString(value, tmpMin) && tmpMin > 0) {
            leaseInMin = tmpMin;
        }
    }
    if (currentTs - fileMeta.lastModifyTime > leaseInMin * 60) {
        // if time gap is over 30 minutes, it's invalid
        if (fs::FileSystem::removeFile(localFilePath) != EC_OK) {
            AUTIL_LOG(ERROR, "remove [%s] failed", localFilePath.c_str());
            return false;
        }
        return true;
    }
    utime(localFilePath.c_str(), NULL);
    string result;
    if (fs::FileSystem::readFile(localFilePath, result) != EC_OK) {
        AUTIL_LOG(ERROR, "read [%s] failed", localFilePath.c_str());
        return false;
    }

    if (!StringUtil::fromString(result, version)) {
        AUTIL_LOG(ERROR, "file [%s] content [%s] parse error", localFilePath.c_str(), result.c_str());
        return false;
    }
    return true;
}

bool LeaderElector::handleVersion() {
    if (_localLockVersionFile.empty()) {
        return true;
    }
    string filePath = getLeaderVersionPath(_leaderPath);

    bool isExist = false;
    if (!_zkWrapper->check(filePath, isExist)) {
        return false;
    }
    if (!isExist && !_zkWrapper->createPath(filePath)) {
        AUTIL_LOG(WARN, "create leader version path[%s] failed", _leaderPath.c_str());
        return false;
    }
    vector<string> child;
    auto ec = _zkWrapper->getChild(filePath, child);
    if (ec != ZOK) {
        return false;
    }
    int64_t remoteVersion = -1;
    for (auto &single : child) {
        int64_t version;
        if (!StringUtil::fromString(single, version)) {
            continue;
        }
        remoteVersion = max(remoteVersion, version);
    }
    if (_localLockVersion != -1 && _localLockVersion == remoteVersion) {
        FileMeta fileMeta;
        auto ec = fs::FileSystem::getFileMeta(_localLockVersionFile, fileMeta);
        if (ec != EC_OK) {
            AUTIL_LOG(ERROR, "check local file [%s] failed", _localLockVersionFile.c_str());
            return false;
        }
        auto currentTs = TimeUtility::currentTimeInSeconds();
        if (currentTs - fileMeta.lastModifyTime > 10ll * 60) {
            // local version file rewrite time interval is 10 min
            utime(_localLockVersionFile.c_str(), NULL);
            // if (EC_OK != fs::FileSystem::writeFile(
            //         _localLockVersionFile, StringUtil::toString(_localLockVersion))) {
            //     AUTIL_LOG(ERROR, "write local file [%s] failed", _localLockVersionFile.c_str());
            //     //exit(1);
            //     raise(10);
            // }
        }
        return true;
    }
    if (_localLockVersion == -1) {
        if (!_zkWrapper->createNode(
                fs::FileSystem::joinFilePath(filePath, StringUtil::toString(remoteVersion + 1)), "", true)) {
            return false;
        }
        AUTIL_LOG(INFO, "create remote version [%ld] success", remoteVersion + 1);
        _localLockVersion = remoteVersion + 1;
        if (EC_OK != fs::FileSystem::writeFile(_localLockVersionFile, StringUtil::toString(_localLockVersion))) {
            AUTIL_LOG(ERROR, "write local file [%s] failed", _localLockVersionFile.c_str());
            exit(1);
        }
    }
    if (_localLockVersion < remoteVersion) {
        _isPreempted = true;
        AUTIL_LOG(ERROR, "local version [%ld] < remote version [%ld]", _localLockVersion, remoteVersion);
        raise(SIGUSR1);
        if (_preemptedHandler) {
            _preemptedHandler();
        } else {
            exit(1);
        }
    }
    return true;
}

}; // namespace worker_framework
