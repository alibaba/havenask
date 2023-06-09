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
#ifndef __INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H
#define __INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, SingleFieldIndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, MultiCounter);

namespace indexlib { namespace index {

class SegmentLockSizeCalculator
{
public:
    SegmentLockSizeCalculator(const config::IndexPartitionSchemaPtr& schema,
                              const plugin::PluginManagerPtr& pluginManager, bool needDedup = false);

    virtual ~SegmentLockSizeCalculator();

public:
    virtual size_t CalculateSize(const file_system::DirectoryPtr& directory,
                                 const util::MultiCounterPtr& counter = util::MultiCounterPtr()) const;
    size_t CalculateDiffSize(const file_system::DirectoryPtr& directory, const std::string& oldTemperature,
                             const std::string& newTemperature) const;

private:
    size_t CalculateIndexSize(const file_system::DirectoryPtr& directory, const util::MultiCounterPtr& counter) const;
    size_t CalculateIndexDiffSize(const file_system::DirectoryPtr& directory, const std::string& oldTemperature,
                                  const std::string& newTemperature) const;
    size_t CalculateAttributeSize(const file_system::DirectoryPtr& directory,
                                  const util::MultiCounterPtr& counter) const;
    size_t CalculateAttributeDiffSize(const file_system::DirectoryPtr& directory, const std::string& oldTemperature,
                                      const std::string& newTemperature) const;

    size_t CalculateSummarySize(const file_system::DirectoryPtr& directory) const;
    size_t CalculateSummaryDiffSize(const file_system::DirectoryPtr& directory, const std::string& oldTemperature,
                                    const std::string& newTemperature) const;
    void GetSummaryDirectory(const file_system::DirectoryPtr& directory,
                             std::vector<file_system::DirectoryPtr>& groupDirs) const;
    size_t CalculateSourceSize(const file_system::DirectoryPtr& directory) const;
    size_t CalculateSourceDiffSize(const file_system::DirectoryPtr& directory, const std::string& oldTemperature,
                                   const std::string& newTemperature) const;
    size_t CalculateNormalIndexSize(const file_system::DirectoryPtr& directory,
                                    const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateNormalIndexDiffSize(const file_system::DirectoryPtr& directory,
                                        const config::IndexConfigPtr& indexConfig, const std::string& oldTemperature,
                                        const std::string& newTemperature) const;

    size_t CalculateRangeIndexSize(const file_system::DirectoryPtr& directory,
                                   const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateCustomizedIndexSize(const file_system::DirectoryPtr& directory,
                                        const config::IndexConfigPtr& indexConfig) const;

    bool HasSectionAttribute(const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateSingleAttributeSize(const file_system::DirectoryPtr& attrDir) const;
    size_t CalculateSingleAttributeDiffSize(const file_system::DirectoryPtr& attrDir, const std::string& oldTemperature,
                                            const std::string& newTemperature) const;

    size_t CalculateKVSize(const file_system::DirectoryPtr& directory, const util::MultiCounterPtr& counter) const;
    size_t CalculateKKVSize(const file_system::DirectoryPtr& directory, const util::MultiCounterPtr& counter) const;
    size_t CalculateCustomTableSize(const file_system::DirectoryPtr& directory,
                                    const util::MultiCounterPtr& counter) const;

    std::string CreateIndexIdentifier(const config::IndexConfigPtr& indexConfig, segmentid_t segId) const;

    std::string CreateAttrIdentifier(const config::AttributeConfigPtr& attrConfig, segmentid_t segId) const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    plugin::PluginManagerPtr mPluginManager;
    mutable std::unordered_map<std::string, size_t> mIndexItemSizeMap;
    bool mNeedDedup;

private:
    friend class SegmentLockSizeCalculatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentLockSizeCalculator);
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H
