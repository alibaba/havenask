#ifndef __INDEXLIB_AND_INDEX_RECLAIMER_H
#define __INDEXLIB_AND_INDEX_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(index, IndexReader);
IE_NAMESPACE_BEGIN(merger);

class AndIndexReclaimer : public IndexReclaimer
{
public:
    AndIndexReclaimer(const index_base::PartitionDataPtr& partitionData,
                      const IndexReclaimerParam& reclaimParam,
                      misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~AndIndexReclaimer();
public:
    bool Reclaim(const DocumentDeleterPtr& docDeleter) override;
    bool Init(const config::IndexSchemaPtr& indexSchema,
              const index::MultiFieldIndexReaderPtr& indexReaders);
private:
    IE_DECLARE_METRIC(AndReclaim);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AndIndexReclaimer);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_AND_INDEX_RECLAIMER_H
