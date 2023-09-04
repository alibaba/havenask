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
#ifndef HIPPO_AMLEADERCHECKER_H
#define HIPPO_AMLEADERCHECKER_H

#include "util/Log.h"
#include "common/common.h"
#include "worker_framework/LeaderChecker.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class AmLeaderChecker
{
public:
    AmLeaderChecker(worker_framework::LeaderChecker * leaderChecker = NULL);
    virtual ~AmLeaderChecker();
private:
    AmLeaderChecker(const AmLeaderChecker &);
    AmLeaderChecker& operator=(const AmLeaderChecker &);
public:
    /* virtual for mock */
    virtual bool createPrivateLeaderChecker(
            const std::string &leaderElectRoot, int64_t sessionTimeout,
            const std::string &baseName, const std::string &myAddr);
    
    bool isLeader();
    worker_framework::LeaderChecker * getLeaderChecker() {
        return _leaderChecker;
    }
    cm_basic::ZkWrapper *getZkWrapper() {
        return _leaderChecker->getZkWrapper();
    }
public: //for test
    void setLeaderChecker(worker_framework::LeaderChecker * leaderChecker,
                          bool isOwner = true)
    {
        if (_leaderChecker && _isOwner) {
            delete _leaderChecker;
        }
        _leaderChecker = leaderChecker;
        _isOwner = isOwner;
    }
private:
    cm_basic::ZkWrapper *_zkWrapper;    
    worker_framework::LeaderChecker * _leaderChecker;
    bool _isOwner;
private:
    static const std::string LEADER_ELECTION_BASE_FILE;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(AmLeaderChecker);

class AmLeaderCheckerCreator {
public:
    virtual ~AmLeaderCheckerCreator() {};
    
public:
    virtual AmLeaderChecker* createAmLeaderChecker(
            worker_framework::LeaderChecker * leaderChecker)
    {
        return new AmLeaderChecker(leaderChecker);
    }
};

HIPPO_INTERNAL_TYPEDEF_PTR(AmLeaderCheckerCreator);

END_HIPPO_NAMESPACE(sdk);

#endif //HIPPO_AMLEADERCHECKER_H
