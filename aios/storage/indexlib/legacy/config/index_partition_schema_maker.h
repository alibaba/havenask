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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class IndexPartitionSchemaMaker
{
public:
    typedef std::vector<std::string> StringVector;

    IndexPartitionSchemaMaker();
    ~IndexPartitionSchemaMaker();

public:
    static void MakeSchema(IndexPartitionSchemaPtr& schema, const std::string& fieldNames,
                           const std::string& indexNames, const std::string& attributeNames,
                           const std::string& summaryNames, const std::string& truncateProfileStr = "");

    static void MakeFieldConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeIndexConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeAttributeConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeSummaryConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeTruncateProfiles(IndexPartitionSchemaPtr& schema, const std::string& truncateProfileStr);

    static StringVector SpliteToStringVector(const std::string& names);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionSchemaMaker> IndexPartitionSchemaMakerPtr;
}} // namespace indexlib::config
