#ifndef __INDEXLIB_LIFECYCLE_SEGMENT_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_LIFECYCLE_SEGMENT_ATTRIBUTE_UPDATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index/max_min_segment_metrics_updater.h"
#include <autil/legacy/any.h>

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(index);

class LifecycleSegmentMetricsUpdater : public SegmentMetricsUpdater
{
public:
    LifecycleSegmentMetricsUpdater();
    ~LifecycleSegmentMetricsUpdater();
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const util::KeyValueMap& parameters) override;
    bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
        const util::KeyValueMap& parameters) override;
public:
    void Update(const document::DocumentPtr& doc) override;
    void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId) override;
    autil::legacy::json::JsonMap Dump() const override;
    
public:
    static const std::string UPDATER_NAME;
    static const std::string LIFECYCLE_PARAM;    
    static const std::string DEFAULT_LIFECYCLE_TAG;

public:
    static std::string GetAttrKey(const std::string& attrName)
    {
        return MaxMinSegmentMetricsUpdater::GetAttrKey(attrName);
    }

private:
    template <typename T>
    bool GetSegmentAttrValue(const index_base::SegmentMetrics& metrics, T& maxValue, T& minValue) const;

    template <typename T> std::string GenerateLifeCycleTag() const;

    template <typename T> bool InitThresholdInfos();

private:
    MaxMinSegmentMetricsUpdater mMaxMinUpdater;
    bool mIsMerge;
    std::string mLifecycleParam;
    std::string mDefaultLifecycleTag;
    index_base::SegmentMergeInfos mSegMergeInfos;
    autil::legacy::Any mThresholdInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LifecycleSegmentMetricsUpdater);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LIFECYCLE_SEGMENT_ATTRIBUTE_UPDATER_H
