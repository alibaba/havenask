#include "aitheta_indexer/plugins/aitheta/test/index_segment_test.h"

#define private public
#include "aitheta_indexer/plugins/aitheta/index_segment.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void IndexSegmentTest::TestMergeRawSegments() {
    const auto &partDir = GET_PARTITION_DIRECTORY();
    string dir("index_segment_test");
    DirectoryPtr dirPtr = partDir->MakeDirectory(dir);
    int32_t embeddingDimension = 8;

    RawSegmentPtr rawSegment0(new RawSegment);
    {
        std::map<int64_t, CategoryRecords> catId2Records;
        vector<int64_t> pkeys = {1, 2};
        vector<docid_t> docids = {1, 2};
        vector<EmbeddingPtr> embeddings = {EmbeddingPtr(new float[8]), EmbeddingPtr(new float[8])};
        int64_t categoryId = 0;
        rawSegment0->mCatId2Records.emplace(categoryId, CategoryRecords(pkeys, docids, embeddings));
        rawSegment0->mDimension = embeddingDimension;
    }

    RawSegmentPtr rawSegment1(new RawSegment);
    {
        std::map<int64_t, CategoryRecords> catId2Records1;
        vector<int64_t> pkeys = {3, 4};
        vector<docid_t> docids = {-1, 2};
        vector<EmbeddingPtr> embeddings = {EmbeddingPtr(new float[8]), EmbeddingPtr(new float[8])};
        int64_t categoryId = 0;
        rawSegment1->mCatId2Records.emplace(categoryId, CategoryRecords(pkeys, docids, embeddings));
        rawSegment1->mDimension = embeddingDimension;
    }

    IndexSegment indexSegment;
    // 原有doc id序列
    *(indexSegment.mDocIdArray) = {0, 1, 2, 3, 4};
    *(indexSegment.mPkeyArray) = {0, 1, 2, 3, 4};
    indexSegment.BuildPkMap4IndexSegment();
    docid_t base = 0;
    vector<docid_t> segmentBaseDocIds = {0, 2};
    ASSERT_EQ(true, indexSegment.MergeRawSegments(base, {rawSegment0, rawSegment1}, segmentBaseDocIds));
    ASSERT_EQ(vector<int>({0, 1, 2, -1, 4}), *(indexSegment.mDocIdArray));
}

void IndexSegmentTest::TestMergeRawSegments2() {
    const auto &partDir = GET_PARTITION_DIRECTORY();
    string dir("index_segment_test");
    DirectoryPtr dirPtr = partDir->MakeDirectory(dir);
    int32_t embeddingDimension = 8;

    RawSegmentPtr rawSegment0(new RawSegment);
    {
        std::map<int64_t, CategoryRecords> catId2Records;
        vector<docid_t> docids = {0, 1};
        vector<int64_t> pkeys = {0, 1};
        vector<EmbeddingPtr> embeddings = {EmbeddingPtr(new float[8]), EmbeddingPtr(new float[8])};
        int64_t categoryId = 0;
        rawSegment0->mCatId2Records.emplace(categoryId, CategoryRecords(pkeys, docids, embeddings));
        rawSegment0->mDimension = embeddingDimension;
    }

    IndexSegment indexSegment;
    // 原有doc id序列
    *(indexSegment.mDocIdArray) = {-1, -1, 0, 1, 2};
    *(indexSegment.mPkeyArray) = {0, 1, 2, 3, 4};
    indexSegment.BuildPkMap4IndexSegment();

    RawSegmentPtr rawSegment1(new RawSegment);
    {
        std::map<int64_t, CategoryRecords> catId2Records1;
        vector<docid_t> docids = {-1, 0, 1};
        vector<int64_t> pkeys = {3, 4, 5};
        vector<EmbeddingPtr> embeddings = {EmbeddingPtr(new float[8]), EmbeddingPtr(new float[8])};
        int64_t categoryId = 0;
        rawSegment1->mCatId2Records.emplace(categoryId, CategoryRecords(pkeys, docids, embeddings));
        rawSegment1->mDimension = embeddingDimension;
    }

    docid_t base = 2;
    vector<docid_t> segmentBaseDocIds = {0, 5};
    ASSERT_EQ(true, indexSegment.MergeRawSegments(base, {rawSegment0, rawSegment1}, segmentBaseDocIds));
    ASSERT_EQ(vector<int>({0 - 2, 1 - 2, 2 - 2, -1, 5 - 2}), *(indexSegment.mDocIdArray));
}

void IndexSegmentTest::TestBuildPkMap4RawSegments() {
    const auto &partitionDir = GET_PARTITION_DIRECTORY();
    vector<RawSegmentPtr> incRawSegmentVec;
    {
        DirectoryPtr dir = partitionDir->MakeDirectory("raw1");
        RawSegmentPtr raw(new RawSegment);
        raw->SetDimension(4);
        EmbeddingPtr embedding(new float[4]{0.1, 0.2, 0.3, 0.4});
        raw->Add(1, 1000, 0, embedding);
        raw->Add(1, 2000, 1, embedding);
        raw->Add(1, 3000, 2, embedding);
        raw->Add(1, 4000, 3, embedding);
        raw->Dump(dir);
        RawSegmentPtr rawSegment(new RawSegment);
        rawSegment->Load(dir);

        auto docIdMap = CreateDocIdMap(0, {INVALID_DOCID, INVALID_DOCID, 2, 3, 4, 5});
        rawSegment->UpdateDocId(docIdMap);

        incRawSegmentVec.push_back(rawSegment);
    }
    {
        DirectoryPtr dir = partitionDir->MakeDirectory("raw2");
        RawSegmentPtr raw(new RawSegment);
        raw->SetDimension(4);
        EmbeddingPtr embedding;
        raw->Add(1, 1000, 0, embedding);
        raw->Add(1, 2000, 1, embedding);
        raw->Dump(dir);
        RawSegmentPtr rawSegment(new RawSegment);
        rawSegment->Load(dir);

        auto docIdMap = CreateDocIdMap(4, {INVALID_DOCID, INVALID_DOCID, 2, 3, 4, 5});
        rawSegment->UpdateDocId(docIdMap);

        incRawSegmentVec.push_back(rawSegment);
    }
    {
        unordered_map<int64_t, std::pair<docid_t, docid_t>> pkMap;
        IndexSegment::BuildPkMap4RawSegments(incRawSegmentVec, {}, pkMap);
        EXPECT_EQ(4, pkMap[1000].second + pkMap[1000].first);
        EXPECT_EQ(5, pkMap[2000].second + pkMap[2000].first);
        EXPECT_EQ(2, pkMap[3000].second + pkMap[3000].first);
        EXPECT_EQ(3, pkMap[4000].second + pkMap[4000].first);
    }
    {
        unordered_map<int64_t, std::pair<docid_t, docid_t>> pkMap;
        IndexSegment::BuildPkMap4RawSegments(incRawSegmentVec, {0, 1024}, pkMap);
        EXPECT_EQ(4 + 1024, pkMap[1000].second + pkMap[1000].first);
        EXPECT_EQ(5 + 1024, pkMap[2000].second + pkMap[2000].first);
        EXPECT_EQ(2, pkMap[3000].second + pkMap[3000].first);
        EXPECT_EQ(3, pkMap[4000].second + pkMap[4000].first);
    }
}

IE_NAMESPACE_END(aitheta_plugin);
