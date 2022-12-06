#include "aitheta_indexer/plugins/aitheta/test/index_segment_builder_test.h"
#include <proxima/common/params_define.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include "aitheta_indexer/plugins/aitheta/index_segment_builder.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

void IndexSegmentBuilderTest::TestMergeInc() {
    const auto &partitionDir = GET_PARTITION_DIRECTORY();
    vector<RawSegmentPtr> fullRawSegmentVec;
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

        fullRawSegmentVec.push_back(rawSegment);
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
    DirectoryPtr dir = partitionDir->MakeDirectory("index");
    util::KeyValueMap kv = {
        {DIMENSION, "4"}, {INDEX_TYPE, INDEX_TYPE_LR}, {BUILD_METRIC_TYPE, INNER_PRODUCT}, {SEARCH_METRIC_TYPE, L2}};
    CommonParam cp(kv);
    KnnConfig knnConfig;
    IndexSegmentBuilder builder(fullRawSegmentVec, cp, kv, knnConfig);
    CustomReduceTaskPtr reduceTask;
    builder.BuildAndDump(dir, reduceTask, incRawSegmentVec);

    IndexSegment indexSegment;
    indexSegment.Load(dir);
    auto &docIdArray = indexSegment.getDocIdArray();
    ASSERT_EQ(4, docIdArray.size());
    ASSERT_EQ(4, docIdArray[0]);
    ASSERT_EQ(5, docIdArray[1]);
    ASSERT_EQ(2, docIdArray[2]);
    ASSERT_EQ(3, docIdArray[3]);
}

void IndexSegmentBuilderTest::TestReducerMergeInc() {
    const auto &partitionDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr dir1 = partitionDir->MakeDirectory("raw1");
    DirectoryPtr dir2 = partitionDir->MakeDirectory("raw2");
    {
        RawSegmentPtr raw(new RawSegment);
        raw->SetDimension(4);
        EmbeddingPtr embedding(new float[4]{0.1, 0.2, 0.3, 0.4});
        raw->Add(1, 1000, 0, embedding);
        raw->Add(1, 2000, 1, embedding);
        raw->Add(1, 3000, 2, embedding);
        raw->Add(1, 4000, 3, embedding);
        raw->Dump(dir1);
    }
    {
        RawSegmentPtr raw(new RawSegment);
        raw->SetDimension(4);
        EmbeddingPtr embedding;
        raw->Add(1, 1000, 0, embedding);
        raw->Add(1, 2000, 1, embedding);
        raw->Dump(dir2);
    }
    util::KeyValueMap params = {
        {DIMENSION, "4"}, {INDEX_TYPE, INDEX_TYPE_LR}, {BUILD_METRIC_TYPE, INNER_PRODUCT}, {SEARCH_METRIC_TYPE, L2}};
    DirectoryPtr mixDir = partitionDir->MakeDirectory("mix");

    auto reducer = mFactory->createIndexReducer(params);
    ASSERT_TRUE(nullptr != reducer);
    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    auto rawSegReduceItem1 = reducer->CreateReduceItem();
    auto rawSegReduceItem2 = reducer->CreateReduceItem();
    vector<docid_t> docIds = {-1, -1, 2, 3, 4, 5};
    {
        ASSERT_TRUE(rawSegReduceItem1->LoadIndex(dir1));
        docid_t baseDocId = 0;
        DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);
        ASSERT_TRUE(rawSegReduceItem1->UpdateDocId(docIdMap));
    }
    {
        ASSERT_TRUE(rawSegReduceItem2->LoadIndex(dir2));
        docid_t baseDocId = 4;
        DocIdMap docIdMap = CreateDocIdMap(baseDocId, docIds);
        ASSERT_TRUE(rawSegReduceItem2->UpdateDocId(docIdMap));
    }
    vector<IndexReduceItemPtr> reduceItems = {rawSegReduceItem1, rawSegReduceItem2};
    vector<DirectoryPtr> loadDirs = {dir1, dir2};

    docid_t base = 0;
    SegmentMergeInfos mergeInfos(
        {SegmentMergeInfo(0, SegmentInfo(), 0U, base), SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
    mergeInfos[0].segmentInfo.docCount = 4;
    mergeInfos[1].segmentInfo.docCount = 2;
    IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);
    OutputSegmentMergeInfos outputSegMergeInfos;
    size_t size = reducer->EstimateMemoryUse(loadDirs, mergeInfos, outputSegMergeInfos, true);
    EXPECT_GE(size, 100);

    OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.directory = mixDir;
    ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, true, rsc));
    IndexSegment indexSegment;
    indexSegment.Load(mixDir);
    auto &docIdArray = indexSegment.getDocIdArray();
    ASSERT_EQ(4, docIdArray[0]);
    ASSERT_EQ(5, docIdArray[1]);
    ASSERT_EQ(2, docIdArray[2]);
    ASSERT_EQ(3, docIdArray[3]);
}

void IndexSegmentBuilderTest::TestReducerMergeEmpty() {
    const auto &partitionDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr dir = partitionDir->MakeDirectory("raw");
    {
        RawSegmentPtr raw(new RawSegment);
        raw->Dump(dir);
    }
    util::KeyValueMap params = {
        {DIMENSION, "4"}, {INDEX_TYPE, INDEX_TYPE_LR}, {BUILD_METRIC_TYPE, INNER_PRODUCT}, {SEARCH_METRIC_TYPE, L2}};
    DirectoryPtr indexDir = partitionDir->MakeDirectory("index");

    auto reducer = mFactory->createIndexReducer(params);
    ASSERT_TRUE(nullptr != reducer);
    IndexerResourcePtr indexResourcePtr(new IndexerResource());
    indexResourcePtr->pluginPath = mPluginRoot;
    ASSERT_TRUE(reducer->DoInit(indexResourcePtr));

    auto rawSegReduceItem = reducer->CreateReduceItem();
    vector<docid_t> docids = {0, 1, 2, 3, 4, 5};
    {
        ASSERT_TRUE(rawSegReduceItem->LoadIndex(dir));
        docid_t baseDocId = 0;
        DocIdMap docIdMap = CreateDocIdMap(baseDocId, docids);
        ASSERT_TRUE(rawSegReduceItem->UpdateDocId(docIdMap));
    }
    vector<IndexReduceItemPtr> reduceItems = {rawSegReduceItem};
    vector<DirectoryPtr> loadDirs = {dir};

    docid_t base = 0;
    SegmentMergeInfos mergeInfos({SegmentMergeInfo(0, SegmentInfo(), 0U, base)});
    mergeInfos[0].segmentInfo.docCount = 0;
    IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), true);

    OutputSegmentMergeInfo outputSegmentMergeInfo;
    outputSegmentMergeInfo.directory = indexDir;
    ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegmentMergeInfo}, true, rsc));
    IndexSegment indexSegment;
    indexSegment.Load(indexDir);
    auto &docIdArray = indexSegment.getDocIdArray();
    ASSERT_TRUE(docIdArray.empty());
}

void IndexSegmentBuilderTest::TestCreateLocalDumpPath() {
    const string expect = "./local_dump_path_" + StringUtil::toString(pthread_self());
    const string actual = IndexSegmentBuilder::CreateLocalDumpPath();
    EXPECT_EQ(0, strncasecmp(expect.c_str(), actual.c_str(), expect.size()));
}

IE_NAMESPACE_END(aitheta_plugin);
