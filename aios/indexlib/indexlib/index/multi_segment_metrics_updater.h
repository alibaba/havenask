#ifndef __INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H
#define __INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H

#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, ModuleClassConfig);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(index);

class MultiSegmentMetricsUpdater : public SegmentMetricsUpdater
{
public:
    using SegmentMetricsUpdaterVector = std::vector<std::unique_ptr<SegmentMetricsUpdater>>;

public:
    MultiSegmentMetricsUpdater();
    ~MultiSegmentMetricsUpdater();
public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options, const util::KeyValueMap& parameters)
    {
        assert(false);
        return false;
    }
    virtual bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
        const util::KeyValueMap& parameters)
    {
        assert(false);
        return false;
    }

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const std::vector<config::ModuleClassConfig> updaterConfigs,
        const plugin::PluginManagerPtr& pluginManager);

    void InitForMerge(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
        const std::vector<config::ModuleClassConfig> updaterConfigs,
        const plugin::PluginManagerPtr& pluginManager);

public:
    virtual void Update(const document::DocumentPtr& doc) override;
    virtual void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId) override;
    virtual autil::legacy::json::JsonMap Dump() const override;
    
private:
    SegmentMetricsUpdaterVector mSegMetricsUpdaters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentMetricsUpdater);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SEGMENT_METRICS_UPDATER_H
