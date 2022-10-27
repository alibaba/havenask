#ifndef __INDEXLIB_OBSOLETE_DOC_RECLAIMER_H
#define __INDEXLIB_OBSOLETE_DOC_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_data_iterator.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(merger, DocumentDeleter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(merger);

class ObsoleteDocReclaimer
{
public:
    ObsoleteDocReclaimer(const config::IndexPartitionSchemaPtr& schema,
                         misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~ObsoleteDocReclaimer();
    
public:
    void Reclaim(const DocumentDeleterPtr& docDeleter,
                 const SegmentDirectoryPtr& segDir);
    void RemoveObsoleteSegments(const SegmentDirectoryPtr& segDir);
private:
    size_t DeleteObsoleteDocs(
        index::SingleValueDataIterator<uint32_t>& iterator,
        exdocid_t baseDocid, const DocumentDeleterPtr& docDeleter);
private:
    config::IndexPartitionSchemaPtr mSchema;
    int64_t mCurrentTime;
    misc::MetricProviderPtr mMetricProvider;
    IE_DECLARE_METRIC(obsoleteDoc);
    IE_DECLARE_METRIC(obsoleteSegment);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ObsoleteDocReclaimer);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_OBSOLETE_DOC_RECLAIMER_H
