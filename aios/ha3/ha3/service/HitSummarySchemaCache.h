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

#include <map>
#include <memory>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/HitSummarySchema.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace service {

class HitSummarySchemaCache
{
public:
    HitSummarySchemaCache(const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr);
    ~HitSummarySchemaCache();
private:
    HitSummarySchemaCache(const HitSummarySchemaCache &);
    HitSummarySchemaCache& operator=(const HitSummarySchemaCache &);
public:
    void setHitSummarySchema(const std::string &clusterName,
                             config::HitSummarySchema* hitSummarySchema);
    config::HitSummarySchemaPtr getHitSummarySchema(const std::string &clusterName) const;

public:
    // only for test
    void addHitSummarySchema(const std::string &clusterName,
                             const config::HitSummarySchema* hitSummarySchema);
private:
    mutable autil::ReadWriteLock _lock;
    std::map<std::string, config::HitSummarySchemaPtr> _hitSummarySchemaMap;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<HitSummarySchemaCache> HitSummarySchemaCachePtr;

} // namespace service
} // namespace isearch

