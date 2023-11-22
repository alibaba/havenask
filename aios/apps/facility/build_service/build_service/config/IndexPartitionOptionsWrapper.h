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
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"

namespace build_service { namespace config {

class IndexPartitionOptionsWrapper
{
public:
    typedef std::map<std::string, indexlib::config::IndexPartitionOptions> IndexPartitionOptionsMap;

public:
    bool load(const ResourceReader& resourceReader, const std::string& clusterName);
    bool hasIndexPartitionOptions(const std::string& configName) const;
    bool getIndexPartitionOptions(const std::string& configName,
                                  indexlib::config::IndexPartitionOptions& indexOptions) const;

    const IndexPartitionOptionsMap& getIndexPartitionOptionsMap() const { return _indexPartitionOptionsMap; }

    static bool getFullMergeConfigName(ResourceReader* resourceReader, const std::string& dataTable,
                                       const std::string& clusterName, std::string& fullMergeConfig);

public:
    static bool getIndexPartitionOptions(const ResourceReader& resourceReader, const std::string& clusterName,
                                         const std::string& configName,
                                         indexlib::config::IndexPartitionOptions& indexOptions);

    static bool initReservedVersions(const KeyValueMap& kvMap, indexlib::config::IndexPartitionOptions& options);

    static void initOperationIds(const KeyValueMap& kvMap, indexlib::config::IndexPartitionOptions& options);

    static void initBatchMask(const KeyValueMap& kvMap, indexlib::config::IndexPartitionOptions& options);

    static void initGenerationId(const KeyValueMap& kvMap, indexlib::config::IndexPartitionOptions& options);

    static void initSchemaUpdateStandard(const KeyValueMap& kvMap, indexlib::config::IndexPartitionOptions& options);

private:
    IndexPartitionOptionsMap _indexPartitionOptionsMap;
    static const std::string DEFAULT_FULL_MERGE_CONFIG;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
