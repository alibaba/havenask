#ifndef __INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H
#define __INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H

#include <tr1/memory>
#include <functional>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/plugin/ModuleFactory.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, SplitSegmentStrategy);
DECLARE_REFERENCE_CLASS(merger, MergePlan);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);

IE_NAMESPACE_BEGIN(merger);

class SplitSegmentStrategyFactory: public plugin::ModuleFactory
{
public:
    SplitSegmentStrategyFactory();
    virtual ~SplitSegmentStrategyFactory();

public:
    static const std::string SPLIT_SEGMENT_STRATEGY_FACTORY_SUFFIX;

public:
    virtual void Init(index::SegmentDirectoryBasePtr segDir,
        config::IndexPartitionSchemaPtr schema,
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos,
        std::function<std::unique_ptr<index::SegmentMetricsUpdater>()> segAttrUpdaterGenerator);

    void destroy() override { delete this; }

public:
    virtual SplitSegmentStrategyPtr CreateSplitStrategy(const std::string& strategyName,
        const util::KeyValueMap& parameters, const MergePlan& plan);

protected:
    index::SegmentDirectoryBasePtr mSegDir;
    config::IndexPartitionSchemaPtr mSchema;
    index::OfflineAttributeSegmentReaderContainerPtr mAttrReaders;
    index_base::SegmentMergeInfos mSegMergeInfos;
    std::function<std::unique_ptr<index::SegmentMetricsUpdater>()> mSegAttrUpdaterGenerator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SplitSegmentStrategyFactory);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SPLIT_SEGMENT_STRATEGY_FACTORY_H
