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
#pragma once

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"

namespace worker_framework {

class LeaderElector {
public:
    typedef std::function<void()> HandlerFuncType;
    static const std::string WF_PREEMPTIVE_LEADER;
    struct LeaderState {
        bool isLeader = false;
        int64_t leaseExpirationTime = -1;
    };

public:
    LeaderElector(cm_basic::ZkWrapper *zkWrapper,
                  const std::string &leaderPath,
                  int64_t leaseInSec = 60,
                  int64_t loopIntervalInSec = 2,
                  const std::string &progressKey = DEFAULT_PROGRESS_KEY,
                  const std::string &workDir = "");
    LeaderElector(cm_basic::ZkWrapper *zkWrapper,
                  const std::string &leaderPath,
                  int64_t leaseInMs,
                  int64_t loopIntervalInMs,
                  bool millisecond,
                  const std::string &progressKey = DEFAULT_PROGRESS_KEY,
                  const std::string &workDir = "");
    virtual ~LeaderElector();

private:
    LeaderElector(const LeaderElector &);
    LeaderElector &operator=(const LeaderElector &);

public:
    // virtual for test
    virtual bool start();
    virtual void stop();

    virtual bool isLeader() const;
    // before this time, we always be leader
    virtual int64_t getLeaseExpirationTime() const;
    virtual bool tryLock();
    virtual void unlock();

    // must call before start
    void setNoLongerLeaderHandler(HandlerFuncType handler);
    void setBecomeLeaderHandler(HandlerFuncType handler);
    void setPreemptedHandler(HandlerFuncType handler);

    void setLeaseInfo(const std::string &leaseInfo);
    void setLeaderInfo(const std::string &leaderInfo);
    void setLeaderInfoPath(const std::string &leaderInfoPath);
    virtual std::string getLeaderInfo() const;
    virtual std::string getLeaderInfoPath() const;

    virtual bool writeLeaderInfo(const std::string &leaderInfoPath, const std::string &leaderInfo);

    cm_basic::ZkWrapper *getZkWrapper() const { return _zkWrapper; }
    LeaderState getLeaderState() const { return _state; };
    void setForbidCampaignLeaderTimeMs(uint64_t forbitTime);
private:
    void workLoop(int64_t currentTime); // for test
private:
    void campaignLeader(int64_t currentTime);
    bool doCampaign(int64_t currentTime);

    void holdLeader(int64_t currentTime);
    void doHoldLeader(int64_t currentTime);

private:
    virtual bool checkStillLeader(int64_t currentTime);
    virtual bool reletLeaderTime(int64_t leaseExpirationTime);
    virtual bool getLeaderLeaseExpirationTime(int64_t &lastLeaderEndTime, std::string &progressKey);
    virtual bool getLeaderEndTimeFromString(const std::string &leaderFileContentStr,
                                            int64_t &lastLeaderEndTime,
                                            std::string &progressKey);

private:
    virtual bool lockForLease();
    virtual bool unlockForLease();

private:
    void updateLeaderInfo(bool toBeLeader, int64_t leaseExpirationTime);
    bool writeForZk(const std::string &zkPath, const std::string &content, bool permanent);
    bool progressKeyEqual(const std::string &progressKey);
    std::string generateLeaderLeaseFileStr(int64_t leaseExpirationTime);
    void checkLeader();

    bool readValidLocalLockFile(const std::string &localFilePath, int64_t &version);
    bool handleVersion();

private:
    static std::string getLockPath(const std::string leaderPath);
    static std::string getLeaseTimeFile(const std::string leaderPath);
    static std::string getLeaderVersionPath(const std::string leaderPath);

private:
    LeaderState _state;
    mutable autil::ThreadMutex _mutex;

    std::string _leaderInfoPath;
    std::string _leaderInfo;
    std::string _leaseInfo;
    mutable autil::ThreadMutex _leaderInfoMutex;

    cm_basic::ZkWrapper *_zkWrapper;
    const std::string _leaderPath;
    int64_t _leaseTimeoutUs;
    int64_t _loopIntervalUs;
    int64_t _localLockVersion;
    int64_t _lastLoopTime = 0;
    uint64_t _forbidCampaignTimeMs = 0;
    std::atomic<int64_t> _nextCanCampaignTime = {0};
    autil::LoopThreadPtr _leaderLockThreadPtr;
    autil::LoopThreadPtr _leaderCheckThreadPtr;
    std::string _progressKey;
    std::string _workDir;
    std::string _localLockVersionFile;

    volatile bool _isPreempted;

    HandlerFuncType _becomeLeaderHandler;
    HandlerFuncType _noLongerLeaderHandler;
    HandlerFuncType _preemptedHandler;

private:
    static const std::string LEADER_LEASE_FILE_NAME;
    static const std::string LEADER_VERSION_FILE_NAME;
    static const std::string LEADER_LEASE_FILE_SPLIT_SIGN;
    static const std::string LOCAL_LOCK_FILE_SPLIT_SIGN;
    static const std::string DEFAULT_PROGRESS_KEY;
};

typedef std::shared_ptr<LeaderElector> LeaderElectorPtr;

}; // namespace worker_framework
