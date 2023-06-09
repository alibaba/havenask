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
#ifndef __INDEXLIB_PARTITION_SIZE_CALCULATOR_H_
#define __INDEXLIB_PARTITION_SIZE_CALCULATOR_H_

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, SegmentLockSizeCalculator);
DECLARE_REFERENCE_CLASS(util, MultiCounter);

namespace indexlib { namespace index {

class PartitionSizeCalculator
{
public:
    PartitionSizeCalculator(const file_system::DirectoryPtr& directory, const config::IndexPartitionSchemaPtr& schema,
                            bool throwExceptionIfNotExist, const plugin::PluginManagerPtr& pluginManager);

    virtual ~PartitionSizeCalculator();

public:
    size_t CalculateDiffVersionLockSizeWithoutPatch(const index_base::Version& version,
                                                    const index_base::Version& lastLoadVersion,
                                                    const index_base::PartitionDataPtr& partitionData,
                                                    const util::MultiCounterPtr& counter = util::MultiCounterPtr());

private:
    virtual SegmentLockSizeCalculatorPtr CreateSegmentCalculator(const config::IndexPartitionSchemaPtr& schema);

    size_t CalculatePkSize(const index_base::PartitionDataPtr& partitionData,
                           const index_base::Version& lastLoadVersion, const util::MultiCounterPtr& counter);
    size_t CalculatePkReaderLoadSize(const config::IndexPartitionSchemaPtr& schema,
                                     const index_base::PartitionDataPtr& partitionData,
                                     const index_base::Version& lastLoadVersion);

    void GetSegmentPathVec(const index_base::Version& version, std::vector<std::string>& segPathVec);
    void AddChangedTemperatueSegment(const index_base::Version& version, const index_base::Version& lastLoadVersion,
                                     std::map<std::string, std::pair<std::string, std::string>>& diffSegPath);

private:
    file_system::DirectoryPtr mDirectory;
    config::IndexPartitionSchemaPtr mSchema;
    bool mThrowExceptionIfNotExist;
    plugin::PluginManagerPtr mPluginManager;

private:
    IE_LOG_DECLARE();

private:
    friend class PartitionSizeCalculatorTest;
};

DEFINE_SHARED_PTR(PartitionSizeCalculator);
}} // namespace indexlib::index

#endif //__INDEXLIB_PARTITION_SIZE_CALCULATOR_H
