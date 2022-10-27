#ifndef __INDEXLIB_INDEX_RETRIEVER_H
#define __INDEXLIB_INDEX_RETRIEVER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/common/term.h"
#include <autil/mem_pool/Pool.h>
#include "indexlib/misc/metric_provider.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentRetriever);

IE_NAMESPACE_BEGIN(index);
class IndexRetriever
{
public:
    IndexRetriever();
    virtual ~IndexRetriever();
public:
    virtual bool Init(const DeletionMapReaderPtr& deletionMapReader,
                      const std::vector<IndexSegmentRetrieverPtr>& retrievers,
                      const std::vector<docid_t>& baseDocIds,
                      const IndexerResourcePtr& resource = IndexerResourcePtr());

    virtual std::vector<SegmentMatchInfo> Search(
            const common::Term& term,
            autil::mem_pool::Pool* pool);

    virtual bool SearchAsync(const common::Term &term,
                             autil::mem_pool::Pool * pool,
                             std::function<void(std::vector<SegmentMatchInfo>)> done);
    
protected:
    index::DeletionMapReaderPtr mDeletionMapReader;
    std::vector<IndexSegmentRetrieverPtr> mSegRetrievers;
    std::vector<docid_t> mBaseDocIds;
    IndexerResourcePtr mIndexerResource;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexRetriever);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_RETRIEVER_H
