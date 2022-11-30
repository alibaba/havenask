#ifndef __INDEXLIB_AITHETA_INDEX_RETRIEVER_H
#define __INDEXLIB_AITHETA_INDEX_RETRIEVER_H

#include "aitheta_indexer/plugins/aitheta/aitheta_index_segment_retriever.h"

#include <memory>

#include <aitheta/index_framework.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h>
#include <indexlib/index/normal/deletionmap/deletion_map_reader.h>
#include <indexlib/indexlib.h>
#include <indexlib/misc/metric_provider.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment_reader.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexRetriever : public IE_NAMESPACE(index)::IndexRetriever {
 public:
    AithetaIndexRetriever(const util::KeyValueMap &parameters) : mComParam(parameters), mSegBaseDocId(INVALID_DOCID) {}
    ~AithetaIndexRetriever(){};

 public:
    bool Init(const IE_NAMESPACE(index)::DeletionMapReaderPtr &deletionMapReader,
              const std::vector<IE_NAMESPACE(index)::IndexSegmentRetrieverPtr> &retrievers,
              const std::vector<docid_t> &baseDocIds,
              const IndexerResourcePtr &resource = IndexerResourcePtr()) override;
    std::vector<SegmentMatchInfo> Search(const common::Term &term, autil::mem_pool::Pool *pool) override;

 private:
    bool ParseQuery(std::string &query, std::vector<QueryInfo> &queryInfos, int &topK, bool &needScoreFilter,
                    float &score);

 private:
    CommonParam mComParam;
    SegmentReaderPtr mSegReader;
    docid_t mSegBaseDocId;

 private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AithetaIndexRetriever);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_RETRIEVER_H
