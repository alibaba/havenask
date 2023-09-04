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
// cache driver requests for carbon proxy
#ifndef CARBON_MASTER_CARBONDRIVERCACHE_H
#define CARBON_MASTER_CARBONDRIVERCACHE_H

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"

BEGIN_CARBON_NAMESPACE(master);
class ProxyClient;
CARBON_TYPEDEF_PTR(ProxyClient);

class CarbonDriverCache
{
public:
    CarbonDriverCache(ProxyClientPtr proxy);
    ~CarbonDriverCache();

    bool start();
    void stop();
    void updateGroupPlans(const GroupPlanMap& plans, ErrorInfo *errorInfo);
    void getGroupStatus(const std::vector<std::string> &groupids, std::vector<GroupStatus> *allGroupStatus) const;

private:
    void commitPlans();
    void runOnce();
    bool loadStatus();

private:
    autil::LoopThreadPtr _threadPtr;
    autil::ThreadMutex _targetLock;
    mutable autil::ReadWriteLock _statusLock;
    ProxyClientPtr _proxy;
    GroupPlanMap _plans;
    std::map<groupid_t, GroupStatus> _statusCache;
    std::string _lastCommitStr;
    int64_t _lastUpdateTm;
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonDriverCache);

END_CARBON_NAMESPACE(master);

#endif
