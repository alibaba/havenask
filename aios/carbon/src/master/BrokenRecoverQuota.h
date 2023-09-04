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
#ifndef CARBON_BROKENRECOVERQUOTA_H
#define CARBON_BROKENRECOVERQUOTA_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/RolePlan.h"

BEGIN_CARBON_NAMESPACE(master);

class BrokenRecoverQuota
{
public:
    BrokenRecoverQuota();
    virtual ~BrokenRecoverQuota();
    
private:
    BrokenRecoverQuota(const BrokenRecoverQuota &);
    BrokenRecoverQuota& operator=(const BrokenRecoverQuota &);
public:
    void updateConfig(const BrokenRecoverQuotaConfig &config);
    
    virtual bool require(int64_t curTime = -1);

private:
    BrokenRecoverQuotaConfig _config;
    std::deque<int64_t> _brokenCounterQueue;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(BrokenRecoverQuota);

END_CARBON_NAMESPACE(master);

#endif //CARBON_BROKENRECOVERQUOTA_H
