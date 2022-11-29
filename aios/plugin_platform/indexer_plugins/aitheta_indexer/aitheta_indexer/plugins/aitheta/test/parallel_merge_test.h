#ifndef __PARALLEL_MERGE_TEST_H
#define __PARALLEL_MERGE_TEST_H

#include "aitheta_indexer/plugins/aitheta/test/aitheta_test_base.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

struct RawSegmentDoc {
    int64_t mCatId;
    int64_t mPkey;
    docid_t mDocId;
    EmbeddingPtr mEmbedding;

    RawSegmentDoc(int64_t catId, int64_t pkey, docid_t docId, const EmbeddingPtr &embedding)
        : mCatId(catId), mPkey(pkey), mDocId(docId), mEmbedding(embedding) {}
};

class ParallelMergeTest : public AithetaTestBase {
 public:
    ParallelMergeTest() = default;
    ~ParallelMergeTest() = default;
 public:
    DECLARE_CLASS_NAME(ParallelMergeTest);
    void TestCreateReduceTask();
    void TestFullMerge();
    void TestIncMerge();
    void TestParallelRawSegMerge();

 private:
    typedef std::vector<RawSegmentDoc> RawSegmentDocVec;

    std::vector<IE_NAMESPACE(file_system)::DirectoryPtr>
        CreateRawSegments(std::vector<RawSegmentDocVec> &rawSegmentDocVecs);

    void ParallelMerge(IE_NAMESPACE(file_system)::DirectoryPtr &parallelSegDir,
                       const util::KeyValueMap &params, const size_t parallelCount,
                       const std::vector<size_t> &docCounts, const DocIdMap &docIdMap,
                       const std::vector<IE_NAMESPACE(file_system)::DirectoryPtr> &segmentDirs,
                       bool isEntireDataSet = true);
    
};

INDEXLIB_UNIT_TEST_CASE(ParallelMergeTest, TestCreateReduceTask);
INDEXLIB_UNIT_TEST_CASE(ParallelMergeTest, TestFullMerge);
INDEXLIB_UNIT_TEST_CASE(ParallelMergeTest, TestIncMerge);
INDEXLIB_UNIT_TEST_CASE(ParallelMergeTest, TestParallelRawSegMerge);

IE_NAMESPACE_END(aitheta_plugin);

#endif
