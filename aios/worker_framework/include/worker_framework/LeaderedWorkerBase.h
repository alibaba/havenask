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
#include "autil/LoopThread.h"
#include "worker_framework/LeaderElector.h"
#include "worker_framework/WorkerBase.h"

namespace worker_framework {

class LeaderedWorkerBaseImpl;

class LeaderedWorkerBase : public WorkerBase {
public:
    LeaderedWorkerBase();
    ~LeaderedWorkerBase();

private:
    LeaderedWorkerBase(const LeaderedWorkerBase &);
    LeaderedWorkerBase &operator=(const LeaderedWorkerBase &);

protected:
    virtual bool isLeader() const;
    virtual cm_basic::ZkWrapper *getZkWrapper() const;
    virtual bool waitToBeLeader(uint32_t retryTimes = LEADER_RETRY_COUNT,
                                uint32_t sleepEachRetry = LEADER_RETRY_TIME_INTERVAL) const;

    virtual bool initLeaderElector(const std::string &zkRoot);
    virtual bool initLeaderElector(const std::string &zkRoot,
                                   int64_t leaseInSec,
                                   int64_t loopInterval,
                                   const std::string &progressKey);
    virtual bool initLeaderElectorNotStart(const std::string &zkRoot,
                                           int64_t leaseInMs,
                                           int64_t loopIntervalMs,
                                           const std::string &progressKey);
    virtual bool startLeaderElector();
    virtual void shutdownLeaderElector();
    virtual LeaderElector *getLeaderElector() const;
    virtual LeaderElector::LeaderState getLeaderState() const;

protected:
    virtual void preempted();
    virtual void becomeLeader() = 0;
    virtual void noLongerLeader() = 0;
    virtual bool needSlotIdInLeaderInfo() const;

protected:
    std::string getLeaderInfo() const;
    std::string getLeaderElectorPath() const;
    std::string getLeaderInfoPath() const;
    std::string getLeaderElectorLockPath() const;
    int32_t getSlotId() const;

private:
    LeaderedWorkerBaseImpl *_impl;

protected:
    static const uint32_t DEFAULT_ZK_TIMEOUT = 10; // in seconds, 10s
    static const uint32_t LEADER_SESSION_TIMEOUT = DEFAULT_ZK_TIMEOUT;
    static const uint32_t LEADER_CHECK_INTERVAL = 500 * 1000;       // 500 ms
    static const uint32_t LEADER_RETRY_TIME_INTERVAL = 1000 * 1000; // 1s
    static const uint32_t LEADER_RETRY_COUNT = 300;
    static const int64_t LEASE_IN_SECOND = 60; // 60s
    static const int64_t LOOP_INTERVAL = 2;    // 2s
    static const std::string LEADER_ELECTION_BASE_FILE;
};

typedef std::shared_ptr<LeaderedWorkerBase> LeaderedWorkerBasePtr;

}; // namespace worker_framework
