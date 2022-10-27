#ifndef __INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H
#define __INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/plugin/ModuleFactory.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index, SegmentMetricsUpdater);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);

IE_NAMESPACE_BEGIN(index);

class SegmentMetricsUpdaterFactory : public plugin::ModuleFactory
{
public:
    SegmentMetricsUpdaterFactory();
    ~SegmentMetricsUpdaterFactory();

public:
    static const std::string SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX;

public:
    virtual std::unique_ptr<index::SegmentMetricsUpdater> CreateUpdater(
        const std::string& className, const util::KeyValueMap& parameters,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options)
        = 0;

    virtual std::unique_ptr<index::SegmentMetricsUpdater> CreateUpdaterForMerge(
        const std::string& className, const util::KeyValueMap& parameters,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders)
        = 0;

    void destroy() override { delete this; }

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_ATTRIBUTE_UPDATER_FACTORY_H
