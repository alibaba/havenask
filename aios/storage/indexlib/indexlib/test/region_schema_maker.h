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
#ifndef __INDEXLIB_REGION_SCHEMA_MAKER_H
#define __INDEXLIB_REGION_SCHEMA_MAKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

class RegionSchemaMaker
{
public:
    RegionSchemaMaker(const std::string& fieldNames, const std::string& tableType);
    ~RegionSchemaMaker();

public:
    void AddRegion(const std::string& regionName, const std::string& pkeyName, const std::string& skeyName,
                   const std::string& valueNames, const std::string& fieldNames, int64_t ttl = INVALID_TTL,
                   bool impactValue = false);

    void AddRegion(const std::string& regionName, const std::string& pkeyName, const std::string& valueNames,
                   const std::string& fieldNames, int64_t ttl = INVALID_TTL, bool impactValue = false);

    const config::IndexPartitionSchemaPtr& GetSchema() const;

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegionSchemaMaker);
}} // namespace indexlib::test

#endif //__INDEXLIB_REGION_SCHEMA_MAKER_H
