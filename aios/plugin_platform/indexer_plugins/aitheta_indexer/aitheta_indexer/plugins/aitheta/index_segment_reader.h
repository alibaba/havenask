#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_READER_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_READER_H

#include <unordered_map>
#include <pthread.h>

#include <autil/ThreadPool.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/parallel_segment.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment_reader.h"
#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"
#include "aitheta_indexer/plugins/aitheta/util/knn_config.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

typedef aitheta::IndexSearcher::Document Document;

struct SearchInfo {
    SearchInfo(int64_t signature, size_t docNum, const IndexSearcherPtr &s, const aitheta::IndexParams &i, bool e,
               const aitheta::MipsReformer &m = aitheta::MipsReformer())
        : mSignature(signature), mDocNum(docNum), mSearcher(s), mIndexParams(i), mMipsEnanble(e), mReformer(m) {}
    int64_t mSignature;
    size_t mDocNum;
    IndexSearcherPtr mSearcher;
    aitheta::IndexParams mIndexParams;
    bool mMipsEnanble;
    aitheta::MipsReformer mReformer;
};

class ContextHolder {
 public:
    ContextHolder() {}
    ~ContextHolder();
    ContextHolder(const ContextHolder &) = delete;
    ContextHolder &operator=(const ContextHolder &) = delete;

 public:
    bool getContext(const SearchInfo &info, aitheta::IndexSearcher::Context *&ret);

 private:
    autil::ReadWriteLock mLock;
    typedef std::unordered_map<int64_t, aitheta::IndexSearcher::Context *> SignatureContext;
    std::unordered_map<pthread_t, SignatureContext> mContext;
};

DEFINE_SHARED_PTR(ContextHolder);

class IndexSegmentReader : public SegmentReader {
 public:
    IndexSegmentReader(const SegmentMeta &meta, const util::KeyValueMap &kvParam, const KnnConfig &knnConfig)
        : SegmentReader(meta),
          mKvParam(kvParam),
          mComParam(mKvParam),
          mKnnConfig(knnConfig),
          mDistType(DistanceType::kUnknown),
          mEnableParallelSearch(false) {}
    ~IndexSegmentReader();

 public:
    bool Open(const IE_NAMESPACE(file_system)::DirectoryPtr &dir) override;

    bool UpdatePkey2DocId(docid_t baseDocId, const std::vector<SegmentReaderPtr> &segmentReaderVec,
                          const std::vector<docid_t> &segmentBaseDocIdVec) override;

    std::vector<MatchInfo> Search(const std::vector<QueryInfo> &queryInfos, autil::mem_pool::Pool *sessionPool,
                                  int32_t topK, bool needScoreFilter = false, float score = 0.0) override;

 private:
    // init
    bool InitDistType();
    bool LoadSegment(const IE_NAMESPACE(file_system)::DirectoryPtr &dir);
    bool InitSearchInfo();
    void InitThreadPool();
    bool InitContextHolder();

 private:
    // search
    void SearchOne(const QueryInfo &qInfo, int32_t topK, std::vector<Document> &docs);
    void SequentialSearch(const std::vector<QueryInfo> &queryInfos, int32_t topK,
                          std::vector<std::vector<Document>> &totalDocs);
    void ParallelSearch(const std::vector<QueryInfo> &queryInfos, int32_t topK,
                        std::vector<std::vector<Document>> &totalDocs);
    // TODO(richard.sy): optimize result transfer: Document -> map<docid_t, float> -> MatchInfo
    void Merge(const std::vector<std::vector<Document>> &totalDocs, const std::vector<QueryInfo> &queryInfos,
               std::map<docid_t, float> &docIds2score) const;
    void Filter(float threshold, std::map<docid_t, float> &docIds2score) const;
    void Format(const std::map<docid_t, float> &docIds2score, autil::mem_pool::Pool *sessionPool,
                MatchInfo &result) const;
    void Report(int64_t startTime, uint32_t queryNum, uint32_t retCntBeforeFilter, uint32_t retCntAfterFilter,
                double recallRatio);

 private:
    util::KeyValueMap mKvParam;
    CommonParam mComParam;
    KnnConfig mKnnConfig;
    DistanceType mDistType;
    std::vector<IndexSegmentPtr> mSegVector;
    std::unordered_map<int64_t, SearchInfo> mCatId2Searcher;
    bool mEnableParallelSearch;
    std::shared_ptr<autil::ThreadPool> mThreadPool;
    ContextHolderPtr mContextHolder;
    std::string mIndexName;

 private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentReader);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_SEGMENT_H
