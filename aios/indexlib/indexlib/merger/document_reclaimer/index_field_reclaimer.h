#ifndef __INDEXLIB_INDEX_FIELD_RECLAIMER_H
#define __INDEXLIB_INDEX_FIELD_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

IE_NAMESPACE_BEGIN(merger);

class IndexFieldReclaimer : public IndexReclaimer
{
public:
    IndexFieldReclaimer(const index_base::PartitionDataPtr& partitionData,
                        const IndexReclaimerParam& reclaimParam,
                        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexFieldReclaimer();
public:
    bool Reclaim(const DocumentDeleterPtr& docDeleter) override;
    bool Init(const config::IndexConfigPtr& indexConfig,
              const index::MultiFieldIndexReaderPtr& indexReaders);
    
private:
    std::vector<std::string> mTerms;
    config::IndexConfigPtr mIndexConfig;
    IE_DECLARE_METRIC(termReclaim);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFieldReclaimer);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_FIELD_RECLAIMER_H
