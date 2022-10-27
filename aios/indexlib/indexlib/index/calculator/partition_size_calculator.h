#ifndef __INDEXLIB_PARTITION_SIZE_CALCULATOR_H_
#define __INDEXLIB_PARTITION_SIZE_CALCULATOR_H_

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, SegmentLockSizeCalculator);
DECLARE_REFERENCE_CLASS(util, MultiCounter);

IE_NAMESPACE_BEGIN(index);

class PartitionSizeCalculator
{
public:
    PartitionSizeCalculator(const file_system::DirectoryPtr& directory,
                            const config::IndexPartitionSchemaPtr& schema,
                            bool throwExceptionIfNotExist,
                            const plugin::PluginManagerPtr& pluginManager);

    virtual ~PartitionSizeCalculator();

public:
    size_t CalculateDiffVersionLockSizeWithoutPatch(
        const index_base::Version& version, const index_base::Version& lastLoadVersion,
        const index_base::PartitionDataPtr& partitionData,
        const util::MultiCounterPtr& counter = util::MultiCounterPtr());

private:
    virtual SegmentLockSizeCalculatorPtr CreateSegmentCalculator(
            const config::IndexPartitionSchemaPtr& schema);

    size_t CalculatePkSize(const index_base::PartitionDataPtr& partitionData,
                           const index_base::Version& lastLoadVersion,
                           const util::MultiCounterPtr& counter);
    size_t CalculatePkReaderLoadSize(
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::PartitionDataPtr& partitionData,
            const index_base::Version& lastLoadVersion);

    void GetSegmentPathVec(const index_base::Version& version,
                           std::vector<std::string>& segPathVec);

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
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PARTITION_SIZE_CALCULATOR_H
