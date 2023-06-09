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
#include "ha3/service/HitSummarySchemaCache.h"

#include <cstddef>
#include <memory>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/config/HitSummarySchema.h"
#include "suez/turing/expression/util/TableInfo.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::config;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, HitSummarySchemaCache);

HitSummarySchemaCache::HitSummarySchemaCache(const ClusterTableInfoMapPtr &clusterTableInfoMapPtr) {
    if (!clusterTableInfoMapPtr) {
        return;
    }
    for (ClusterTableInfoMap::const_iterator it = clusterTableInfoMapPtr->begin();
         it != clusterTableInfoMapPtr->end(); ++it)
    {
        const string &clusterName = it->first;
        HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(it->second.get()));
        _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
    }
}

HitSummarySchemaCache::~HitSummarySchemaCache() {
}

void HitSummarySchemaCache::setHitSummarySchema(const string &clusterName,
        HitSummarySchema* hitSummarySchema)
{
    HitSummarySchemaPtr curHitSummarySchemaPtr = getHitSummarySchema(clusterName);

    if (NULL != curHitSummarySchemaPtr.get()
        && hitSummarySchema->getSignature() == curHitSummarySchemaPtr->getSignatureNoCalc())
    {
        return;
    }

    HitSummarySchemaPtr hitSummarySchemaPtr(hitSummarySchema->clone());
    ScopedWriteLock lock(_lock);
    _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
}

HitSummarySchemaPtr HitSummarySchemaCache::getHitSummarySchema(
        const string &clusterName) const
{
    ScopedReadLock lock(_lock);
    map<string, HitSummarySchemaPtr>::const_iterator it =
        _hitSummarySchemaMap.find(clusterName);
    if (it == _hitSummarySchemaMap.end()) {
        return HitSummarySchemaPtr();
    }
    return it->second;
}

void HitSummarySchemaCache::addHitSummarySchema(const string &clusterName,
        const HitSummarySchema* hitSummarySchema)
{
    HitSummarySchemaPtr hitSummarySchemaPtr(hitSummarySchema->clone());
    _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
}

} // namespace service
} // namespace isearch
