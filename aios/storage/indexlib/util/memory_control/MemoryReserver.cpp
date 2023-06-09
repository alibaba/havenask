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
#include "indexlib/util/memory_control/MemoryReserver.h"

#include "autil/UnitUtil.h"
using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MemoryReserver);

MemoryReserver::MemoryReserver(const string& name, const BlockMemoryQuotaControllerPtr& memController)
    : _name(name)
    , _memController(memController)
{
}

MemoryReserver::~MemoryReserver() { _memController->ShrinkToFit(); }

bool MemoryReserver::Reserve(int64_t quota)
{
    if (quota == 0) {
        AUTIL_LOG(INFO, "[%s] Reserve [0], OK", _name.c_str());
        return true;
    }
    string partitionName;
    int64_t partitionUsedQuota = 0;
    const auto& partMemController = _memController->GetPartitionMemoryQuotaController();
    if (partMemController) {
        partitionName = partMemController->GetName();
        partitionUsedQuota = partMemController->GetUsedQuota();
    } else {
        const auto& partMemControllerV2 = _memController->GetPartitionMemoryQuotaControllerV2();
        partitionName = partMemControllerV2->GetName();
        partitionUsedQuota = partMemControllerV2->GetAllocatedQuota();
    }
    AUTIL_LOG(INFO,
              "[%s] Reserve [%s], current FreeQuota [%s] "
              "Partition[%s] used quota [%s], current block[%s] used quota [%s]",
              _name.c_str(), autil::UnitUtil::GiBDebugString(quota).c_str(),
              autil::UnitUtil::GiBDebugString(_memController->GetFreeQuota()).c_str(), partitionName.c_str(),
              autil::UnitUtil::GiBDebugString(partitionUsedQuota).c_str(), _memController->GetName().c_str(),
              autil::UnitUtil::GiBDebugString(_memController->GetUsedQuota()).c_str());
    if (_memController->Reserve(quota)) {
        AUTIL_LOG(INFO, "Reserve quota [%s] sucuess, FreeQuota [%s]", autil::UnitUtil::GiBDebugString(quota).c_str(),
                  autil::UnitUtil::GiBDebugString(_memController->GetFreeQuota()).c_str());
        return true;
    }
    AUTIL_LOG(WARN, "Reserve quota [%s] failed, FreeQuota [%s]", autil::UnitUtil::GiBDebugString(quota).c_str(),
              autil::UnitUtil::GiBDebugString(_memController->GetFreeQuota()).c_str());
    return false;
}
}} // namespace indexlib::util
