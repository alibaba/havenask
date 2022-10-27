#ifndef __INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H
#define __INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H

#include <tr1/memory>
#include <unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, SingleFieldIndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, MultiCounter);

IE_NAMESPACE_BEGIN(index);

class SegmentLockSizeCalculator
{
public:
    SegmentLockSizeCalculator(
            const config::IndexPartitionSchemaPtr& schema,
            const plugin::PluginManagerPtr& pluginManager,
            bool needDedup = false);

    virtual ~SegmentLockSizeCalculator();

public:
    virtual size_t CalculateSize(
            const file_system::DirectoryPtr& directory,
            const util::MultiCounterPtr& counter = util::MultiCounterPtr()) const;

private:
    size_t CalculateIndexSize(const file_system::DirectoryPtr& directory,
                              const util::MultiCounterPtr& counter) const;
    size_t CalculateAttributeSize(const file_system::DirectoryPtr& directory,
                                  const util::MultiCounterPtr& counter) const;
    size_t CalculateSummarySize(const file_system::DirectoryPtr& directory) const;
    size_t CalculateNormalIndexSize(const file_system::DirectoryPtr& directory,
                                    const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateRangeIndexSize(const file_system::DirectoryPtr& directory,
                                    const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateCustomizedIndexSize(const file_system::DirectoryPtr& directory,
            const config::IndexConfigPtr& indexConfig) const;

    bool HasSectionAttribute(const config::IndexConfigPtr& indexConfig) const;
    size_t CalculateSingleAttributeSize(
        const file_system::DirectoryPtr& attrDir) const;

    size_t CalculateCustomTableSize(const file_system::DirectoryPtr& directory,
                                    const util::MultiCounterPtr& counter) const;

    std::string CreateIndexIdentifier(const config::IndexConfigPtr& indexConfig,
            segmentid_t segId) const;

    std::string CreateAttrIdentifier(const config::AttributeConfigPtr& attrConfig,
            segmentid_t segId) const;

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_LOCK_SIZE_CALCULATOR_H
