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
#ifndef ISEARCH_MULTI_CALL_GIGSTATISTIC_H
#define ISEARCH_MULTI_CALL_GIGSTATISTIC_H

#include <unordered_map>

#include "aios/network/gig/multi_call/agent/WarmUpStrategy.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"

namespace multi_call {

class BizStat;

class GigStatistic
{
public:
    GigStatistic(const std::string &logPrefix = "");
    ~GigStatistic();
    std::shared_ptr<BizStat> getBizStat(const std::string &bizName,
                                        PartIdTy partId = INVALID_PART_ID);
    WarmUpStrategy *getWarmUpStrategy(const std::string &warmUpStrategy);
    void updateWarmUpStrategy(const std::string &warmUpStrategy, int64_t timeoutInSecond,
                              int64_t queryCountLimit);
    void init();
    void start();
    void stop();
    bool stopped();
    void start(const std::string &bizName, PartIdTy partId = INVALID_PART_ID);
    void stop(const std::string &bizName, PartIdTy partId = INVALID_PART_ID);
    bool stopped(const std::string &bizName, PartIdTy partId = INVALID_PART_ID);
    bool longTimeNoQuery(int64_t second);
    bool longTimeNoQuery(const std::string &bizName, int64_t second);
    const std::string &getLogPrefix() const;

private:
    std::shared_ptr<BizStat> newBizStat();
    void updateStartStatAll(bool started);
    void updateStartStat(const std::string &bizName, PartIdTy partId, bool started);
    void logThread();
    std::vector<std::shared_ptr<BizStat>> getBizStatVec(const std::string &bizName) const;

private:
    static bool longTimeNoQuery(const std::vector<std::shared_ptr<BizStat>> &statVec,
                                int64_t second);
    typedef std::unordered_map<std::string, std::unordered_map<PartIdTy, std::shared_ptr<BizStat>>>
        BizStatMap;

private:
    std::string _logPrefix;
    mutable autil::ReadWriteLock _lock;
    // started stat for nonexist biz
    bool _otherBizStarted;
    BizStatMap _statMap;
    autil::ReadWriteLock _warmUpStrategyMapLock;
    std::unordered_map<std::string, WarmUpStrategy *> _warmUpStrategyMap;
    autil::LoopThreadPtr _logThreadPtr;
    std::string _logStr;

private:
    static const uint32_t UPDATE_TIME_INTERVAL = 2 * 1000 * 1000; // 2s
    static const size_t LOG_COUNT = 3000;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTATISTIC_H
