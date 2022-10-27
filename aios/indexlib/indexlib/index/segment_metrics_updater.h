#ifndef __SEGMENT_ATTRIBUTE_UPDATER_H
#define __SEGMENT_ATTRIBUTE_UPDATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include <autil/legacy/jsonizable.h>

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);

IE_NAMESPACE_BEGIN(index);

class SegmentMetricsUpdater {
public:
    SegmentMetricsUpdater() {}
    virtual ~SegmentMetricsUpdater() {}
    
public:
    virtual bool Init(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options, const util::KeyValueMap& parameters)
        = 0;
    virtual bool InitForMerge(const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders,
        const util::KeyValueMap& parameters)
        = 0;

public:
    virtual void Update(const document::DocumentPtr& doc) = 0;
    virtual void UpdateForMerge(segmentid_t oldSegId, docid_t oldLocalId) = 0;
    virtual autil::legacy::json::JsonMap Dump() const = 0;
};

DEFINE_SHARED_PTR(SegmentMetricsUpdater);

IE_NAMESPACE_END(index);

#endif // __SEGMENT_ATTRIBUTE_UPDATER_H










