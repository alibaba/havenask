#include "aitheta_indexer/plugins/aitheta/test/parallel_merge_test.h"

#include <proxima/common/params_define.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include "aitheta_indexer/plugins/aitheta/index_segment_builder.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reduce_item.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_segment_retriever.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

void ParallelMergeTest::TestCreateReduceTask() {
    const auto &patitionDir = GET_PARTITION_DIRECTORY();

    util::KeyValueMap kv = {{DIMENSION, "4"},
                            {INDEX_TYPE, INDEX_TYPE_HC},
                            {BUILD_METRIC_TYPE, INNER_PRODUCT},
                            {SEARCH_METRIC_TYPE, INNER_PRODUCT}};
    EmbeddingPtr embedding1(new float[4]{0.1, 0.2, 0.3, 0.4});
    RawSegmentDocVec raw1 = {RawSegmentDoc(1, 1000, 0, embedding1), RawSegmentDoc(1, 2000, 1, embedding1),
                             RawSegmentDoc(2, 3000, 2, embedding1), RawSegmentDoc(2, 4000, 3, embedding1)};
    EmbeddingPtr embedding2(new float[4]{0.5, 0.6, 0.7, 0.8});
    RawSegmentDocVec raw2 = {RawSegmentDoc(2, 5000, 4, embedding2), RawSegmentDoc(2, 6000, 5, embedding2),
                             RawSegmentDoc(3, 7000, 6, embedding2), RawSegmentDoc(3, 8000, 7, embedding2)};
    std::vector<RawSegmentDocVec> raws = {raw1, raw2};
    std::vector<DirectoryPtr> rawSegDirs = CreateRawSegments(raws);

    SegmentMergeInfos mergeInfos(
        {SegmentMergeInfo(0, SegmentInfo(), 0U, 0), SegmentMergeInfo(1, SegmentInfo(), 0U, 0)});
    mergeInfos[0].segmentInfo.docCount = 4;
    mergeInfos[1].segmentInfo.docCount = 4;

    IndexReducerPtr reducer(new AithetaIndexReducer(kv));
    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager);
    DirectoryPtr resourceDir = patitionDir->MakeDirectory("resource");
    resMgr->Init(resourceDir->GetPath());
    vector<ReduceTask> reduceTasks = reducer->CreateReduceTasks(rawSegDirs, mergeInfos, 2, true, resMgr);

    ASSERT_EQ(2, reduceTasks.size());
    ASSERT_EQ(1, reduceTasks[0].resourceIds.size());
    ASSERT_EQ(1, reduceTasks[1].resourceIds.size());

    MergeTaskResourcePtr taskResource;
    {
        taskResource = resMgr->GetResource(reduceTasks[0].resourceIds[0]);
        string taskJson(taskResource->GetData(), taskResource->GetDataLen());
        CustomReduceTaskPtr task(new CustomReduceTask);
        autil::legacy::FromJsonString(*task, taskJson);
        ASSERT_EQ(true, task->categorySet.find(2) != task->categorySet.end());
    }
    {
        taskResource = resMgr->GetResource(reduceTasks[1].resourceIds[0]);
        string taskJson(taskResource->GetData(), taskResource->GetDataLen());
        CustomReduceTaskPtr task(new CustomReduceTask);
        autil::legacy::FromJsonString(*task, taskJson);
        ASSERT_EQ(true, task->categorySet.find(1) != task->categorySet.end());
        ASSERT_EQ(true, task->categorySet.find(3) != task->categorySet.end());
    }
}

void ParallelMergeTest::TestFullMerge() {
    const auto &patitionDir = GET_PARTITION_DIRECTORY();

    util::KeyValueMap kv = {{DIMENSION, "4"},
                            {INDEX_TYPE, INDEX_TYPE_LR},
                            {BUILD_METRIC_TYPE, INNER_PRODUCT},
                            {SEARCH_METRIC_TYPE, INNER_PRODUCT}};
    EmbeddingPtr embedding1(new float[4]{1, 2, 3, 4});
    EmbeddingPtr embedding2(new float[4]{2, 3, 4, 5});
    RawSegmentDocVec raw1 = {RawSegmentDoc(1, 1000, 0, embedding1), RawSegmentDoc(1, 2000, 1, embedding2),
                             RawSegmentDoc(2, 3000, 2, embedding1), RawSegmentDoc(2, 4000, 3, embedding2)};
    EmbeddingPtr embedding3(new float[4]{3, 4, 5, 6});
    EmbeddingPtr embedding4(new float[4]{4, 5, 6, 7});
    RawSegmentDocVec raw2 = {RawSegmentDoc(2, 5000, 4, embedding3), RawSegmentDoc(2, 6000, 5, embedding4),
                             RawSegmentDoc(3, 7000, 6, embedding3), RawSegmentDoc(3, 8000, 7, embedding4)};
    std::vector<RawSegmentDocVec> raws = {raw1, raw2};
    std::vector<DirectoryPtr> rawSegDirs = CreateRawSegments(raws);

    DirectoryPtr parallelSegDir;
    std::vector<size_t> docCounts = {4, 4};
    vector<docid_t> docIds = {0, 1, 2, 3, 4, 5, 6, 7};
    DocIdMap docIdMap = CreateDocIdMap(0, docIds);
    ParallelMerge(parallelSegDir, kv, 2, docCounts, docIdMap, rawSegDirs);

    IndexRetrieverPtr indexRetriever(new AithetaIndexRetriever(kv));
    IndexSegmentRetrieverPtr indexSegRetriever(new AithetaIndexSegmentRetriever(kv));
    EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(parallelSegDir->GetPath()),
              indexSegRetriever->EstimateLoadMemoryUse(parallelSegDir));

    indexSegRetriever->Open(parallelSegDir);
    std::vector<IndexSegmentRetrieverPtr> retrievers;
    retrievers.push_back(indexSegRetriever);
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    std::vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    indexRetriever->Init(deletionMapReader, retrievers, baseDocIds);

    std::string q;
    std::string query;
    std::string topK;
    common::Term term;
    autil::mem_pool::Pool pool;
    vector<SegmentMatchInfo> segMatchInfos;
    MatchInfoPtr matchInfo;
    float score = 0.0F;

    q = "1#1.0,1.0,1.0,1.0";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);

    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);

    EXPECT_EQ(1, matchInfo->docIds[0]);
    score = 0.0f;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(14.0f, score);

    q = "2#1.0,1.0,1.0,1.0";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);

    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);

    EXPECT_EQ(5, matchInfo->docIds[0]);
    score = 0.0f;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(22.0f, score);

    q = "3#1.0,1.0,1.0,1.0";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);

    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);

    EXPECT_EQ(7, matchInfo->docIds[0]);
    score = 0.0f;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(22.0f, score);
}

void ParallelMergeTest::TestIncMerge() {
    const auto &patitionDir = GET_PARTITION_DIRECTORY();

    util::KeyValueMap kv = {{DIMENSION, "4"},
                            {INDEX_TYPE, INDEX_TYPE_HC},
                            {BUILD_METRIC_TYPE, INNER_PRODUCT},
                            {SEARCH_METRIC_TYPE, INNER_PRODUCT}};
    EmbeddingPtr embedding1(new float[4]{1, 2, 3, 4});
    EmbeddingPtr embedding2(new float[4]{2, 3, 4, 5});
    RawSegmentDocVec raw1 = {RawSegmentDoc(1, 1000, 0, embedding1), RawSegmentDoc(1, 2000, 1, embedding2),
                             RawSegmentDoc(2, 3000, 2, embedding1), RawSegmentDoc(2, 4000, 3, embedding2)};
    EmbeddingPtr embedding3(new float[4]{3, 4, 5, 6});
    EmbeddingPtr embedding4(new float[4]{4, 5, 6, 7});
    RawSegmentDocVec raw2 = {RawSegmentDoc(2, 5000, 4, embedding3), RawSegmentDoc(2, 6000, 5, embedding4),
                             RawSegmentDoc(3, 7000, 6, embedding3), RawSegmentDoc(3, 8000, 7, embedding4)};
    std::vector<RawSegmentDocVec> raws1 = {raw1, raw2};
    std::vector<DirectoryPtr> rawSegDirs1 = CreateRawSegments(raws1);

    DirectoryPtr parallelSegDir1;
    std::vector<size_t> docCounts1 = {4, 4};
    vector<docid_t> docIds1 = {0, 1, 2, 3, 4, 5, 6, 7};
    DocIdMap docIdMap1 = CreateDocIdMap(0, docIds1);
    ParallelMerge(parallelSegDir1, kv, 2, docCounts1, docIdMap1, rawSegDirs1);

    ASSERT_TRUE(nullptr != parallelSegDir1);

    EmbeddingPtr embedding;
    RawSegmentDocVec raw = {RawSegmentDoc(1, 2000, 8, embedding), RawSegmentDoc(2, 4000, 9, embedding)};
    std::vector<RawSegmentDocVec> raws2 = {raw};
    std::vector<DirectoryPtr> rawSegDirs2 = CreateRawSegments(raws2);

    DirectoryPtr parallelSegDir2;
    std::vector<size_t> docCounts2 = {8, 2};
    vector<docid_t> docIds2 = {0, -1, 1, -1, 2, -1, 3, -1, 4, 5};
    DocIdMap docIdMap2 = CreateDocIdMap(0, docIds2);

    rawSegDirs2.push_back(parallelSegDir1);
    ParallelMerge(parallelSegDir2, kv, 2, docCounts2, docIdMap2, rawSegDirs2);

    IndexRetrieverPtr indexRetriever(new AithetaIndexRetriever(kv));
    IndexSegmentRetrieverPtr indexSegRetriever(new AithetaIndexSegmentRetriever(kv));

    EXPECT_EQ(InnerGetIndexSegmentLoad4RetrieveMemoryUse(parallelSegDir2->GetPath()),
              indexSegRetriever->EstimateLoadMemoryUse(parallelSegDir2));
    indexSegRetriever->Open(parallelSegDir2);

    std::vector<IndexSegmentRetrieverPtr> retrievers;
    retrievers.push_back(indexSegRetriever);
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
    std::vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    indexRetriever->Init(deletionMapReader, retrievers, baseDocIds);

    std::string q;
    std::string query;
    std::string topK;
    common::Term term;
    autil::mem_pool::Pool pool;
    vector<SegmentMatchInfo> segMatchInfos;
    MatchInfoPtr matchInfo;
    float score = 0.0F;

    q = "1#1.0,1.0,1.0,1.0";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);
    ASSERT_LE(1u, segMatchInfos.size());
    matchInfo = segMatchInfos[0].matchInfo;
    ASSERT_EQ(1, matchInfo->matchCount);

    EXPECT_EQ(4, matchInfo->docIds[0]);
    score = 0.0f;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(14.0f, score);

    q = "2#1.0,1.0,1.0,1.0";
    topK = "2";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);

    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(1, matchInfo->matchCount);

    EXPECT_EQ(2, matchInfo->docIds[0]);
    score = 0.0f;
    memcpy(&score, matchInfo->matchValues + 0, sizeof(score));
    EXPECT_FLOAT_EQ(18.0f, score);

    q = "3#1.0,1.0,1.0,1.0";
    topK = "1";
    query = q + "&n=" + topK;
    term.SetWord(query);
    segMatchInfos = indexRetriever->Search(term, &pool);

    matchInfo = segMatchInfos[0].matchInfo;
    EXPECT_EQ(0, matchInfo->matchCount);
}

void ParallelMergeTest::TestParallelRawSegMerge() {
    const auto &patitionDir = GET_PARTITION_DIRECTORY();

    util::KeyValueMap kv = {{DIMENSION, "4"},
                            {INDEX_TYPE, INDEX_TYPE_HC},
                            {BUILD_METRIC_TYPE, INNER_PRODUCT},
                            {SEARCH_METRIC_TYPE, INNER_PRODUCT}};
    EmbeddingPtr embedding1(new float[4]{0.1, 0.2, 0.3, 0.4});
    RawSegmentDocVec raw1 = {RawSegmentDoc(1, 1000, 0, embedding1), RawSegmentDoc(1, 2000, 1, embedding1),
                             RawSegmentDoc(2, 3000, 2, embedding1), RawSegmentDoc(2, 4000, 3, embedding1)};
    EmbeddingPtr embedding2(new float[4]{0.5, 0.6, 0.7, 0.8});
    RawSegmentDocVec raw2 = {RawSegmentDoc(2, 5000, 4, embedding2), RawSegmentDoc(2, 6000, 5, embedding2),
                             RawSegmentDoc(3, 7000, 6, embedding2), RawSegmentDoc(3, 8000, 7, embedding2)};
    std::vector<RawSegmentDocVec> raws1 = {raw1, raw2};
    std::vector<DirectoryPtr> rawSegDirs1 = CreateRawSegments(raws1);

    DirectoryPtr parallelSegDir1;
    std::vector<size_t> docCounts1 = {4, 4};
    vector<docid_t> docIds1 = {0, 1, 2, 3, 4, 5, 6, 7};
    DocIdMap docIdMap1 = CreateDocIdMap(0, docIds1);
    ParallelMerge(parallelSegDir1, kv, 2, docCounts1, docIdMap1, rawSegDirs1, false);

    ASSERT_TRUE(nullptr != parallelSegDir1);

    EmbeddingPtr embedding;
    RawSegmentDocVec raw = {RawSegmentDoc(1, 2000, 8, embedding), RawSegmentDoc(2, 4000, 9, embedding)};
    std::vector<RawSegmentDocVec> raws2 = {raw};
    std::vector<DirectoryPtr> rawSegDirs2 = CreateRawSegments(raws2);

    DirectoryPtr parallelSegDir2;
    std::vector<size_t> docCounts2 = {8, 2};
    vector<docid_t> docIds2 = {0, -1, 1, -1, 2, -1, 3, -1, 4, 5};
    DocIdMap docIdMap2 = CreateDocIdMap(0, docIds2);

    rawSegDirs2.insert(rawSegDirs2.begin(), parallelSegDir1);
    ParallelMerge(parallelSegDir2, kv, 2, docCounts2, docIdMap2, rawSegDirs2, false);

    AithetaIndexReduceItemPtr reduceItem(new AithetaIndexReduceItem());
    ASSERT_TRUE(reduceItem->LoadIndex(parallelSegDir2));

    auto rawSegment = DYNAMIC_POINTER_CAST(RawSegment, reduceItem->GetSegment());
    ASSERT_TRUE(rawSegment);
    auto &catId2DocIds = rawSegment->GetCatId2Records();
    EXPECT_EQ(3U, catId2DocIds.size());
    auto it = catId2DocIds.begin();
    EXPECT_EQ(1, it->first);
    const auto &firstDocIds = get<1>(it->second);
    const auto &firstPkeys = get<0>(it->second);
    EXPECT_EQ(2U, firstDocIds.size());
    EXPECT_EQ(4, firstDocIds[0]);
    EXPECT_EQ(2000, firstPkeys[0]);
    EXPECT_EQ(0, firstDocIds[1]);
    EXPECT_EQ(1000, firstPkeys[1]);
    ++it;
    EXPECT_EQ(2, it->first);
    const auto &secondDocIds = get<1>(it->second);
    const auto &secondPkeys = get<0>(it->second);
    EXPECT_EQ(4U, secondDocIds.size());
    EXPECT_EQ(5, secondDocIds[0]);
    EXPECT_EQ(4000, secondPkeys[0]);
    EXPECT_EQ(2, secondDocIds[1]);
    EXPECT_EQ(5000, secondPkeys[1]);
    EXPECT_EQ(-1, secondDocIds[2]);
    EXPECT_EQ(6000, secondPkeys[2]);
    EXPECT_EQ(1, secondDocIds[3]);
    EXPECT_EQ(3000, secondPkeys[3]);
    ++it;
    EXPECT_EQ(3, it->first);
    const auto &thirdDocIds = get<1>(it->second);
    const auto &thirdPkeys = get<0>(it->second);
    EXPECT_EQ(2U, thirdDocIds.size());
    EXPECT_EQ(3, thirdDocIds[0]);
    EXPECT_EQ(7000, thirdPkeys[0]);
    EXPECT_EQ(-1, thirdDocIds[1]);
    EXPECT_EQ(8000, thirdPkeys[1]);
}

std::vector<DirectoryPtr> ParallelMergeTest::CreateRawSegments(std::vector<RawSegmentDocVec> &rawSegmentDocVecs) {
    static size_t no = 0;
    const auto &patitionDir = GET_PARTITION_DIRECTORY();

    vector<DirectoryPtr> rawSegDirs;
    for (size_t i = 0; i < rawSegmentDocVecs.size(); ++i) {
        DirectoryPtr dir = patitionDir->MakeDirectory(string("raw") + autil::StringUtil::toString(no + i));
        RawSegmentPtr raw(new RawSegment);
        raw->SetDimension(4);

        for (size_t j = 0; j < rawSegmentDocVecs[i].size(); ++j) {
            RawSegmentDoc &doc = rawSegmentDocVecs[i][j];
            raw->Add(doc.mCatId, doc.mPkey, doc.mDocId, doc.mEmbedding);
        }

        raw->Dump(dir);
        rawSegDirs.push_back(dir);
    }

    no += rawSegmentDocVecs.size();
    return rawSegDirs;
}

void ParallelMergeTest::ParallelMerge(DirectoryPtr &parallelSegDir, const util::KeyValueMap &kv,
                                      const size_t parallelCount, const std::vector<size_t> &docCounts,
                                      const DocIdMap &docIdMap, const std::vector<DirectoryPtr> &segmentDirs,
                                      bool isEntireDataSet) {
    static size_t no = 0;
    const auto patitionDir = GET_PARTITION_DIRECTORY();
    const auto indexDir = patitionDir->MakeDirectory(string("index") + autil::StringUtil::toString(no));
    const auto resourceDir = patitionDir->MakeDirectory("resource");

    /* Create Reducer */
    IndexReducerPtr reducer(new AithetaIndexReducer(kv));

    /* Create Reduce Tasks */
    SegmentMergeInfos mergeInfos;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        SegmentMergeInfo mergeInfo(i, SegmentInfo(), 0U, 0);
        mergeInfo.segmentInfo.docCount = docCounts[i];
        mergeInfos.push_back(mergeInfo);
    }

    MergeTaskResourceManagerPtr resMgr(new MergeTaskResourceManager);
    resMgr->Init(resourceDir->GetPath());
    vector<ReduceTask> reduceTasks =
        reducer->CreateReduceTasks(segmentDirs, mergeInfos, parallelCount, isEntireDataSet, resMgr);

    /* Run Reduce Tasks */
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    IndexPartitionOptions options = IndexPartitionOptions();
    CounterMapPtr counterMap(new CounterMap());
    IndexerResourcePtr indexResource(
        new IndexerResource(schema, options, counterMap, PartitionMeta(), "index", mPluginRoot));

    IE_NAMESPACE(index)::ReduceResource rsc(mergeInfos, ReclaimMapPtr(new ReclaimMap), isEntireDataSet);

    ParallelReduceMeta meta(reduceTasks.size());
    std::vector<MergeTaskResourceVector> instResourceVec;
    for (size_t i = 0; i < reduceTasks.size(); ++i) {
        IE_LOG(INFO, "reduce task [%lu], total task[%lu]", i, reduceTasks.size());
        MergeItemHint hint(i, reduceTasks[i].dataRatio, 1, parallelCount);
        MergeTaskResourceVector taskResources;
        MergeTaskResourcePtr mergeTaskResource = resMgr->GetResource(reduceTasks[i].resourceIds[0]);

        taskResources.push_back(mergeTaskResource);
        instResourceVec.push_back(taskResources);
        ASSERT_TRUE(reducer->Init(indexResource, hint, taskResources));

        std::vector<IndexReduceItemPtr> reduceItems;
        for (size_t j = 0; j < segmentDirs.size(); ++j) {
            IndexReduceItemPtr reduceItem(reducer->CreateReduceItem());
            ASSERT_TRUE(reduceItem->LoadIndex(segmentDirs[j]));
            ASSERT_TRUE(reduceItem->UpdateDocId(docIdMap));
            reduceItems.push_back(reduceItem);
        }

        OutputSegmentMergeInfo outputSegMergeInfo;
        outputSegMergeInfo.directory = indexDir->MakeDirectory(meta.GetInsDirName(i));

        ASSERT_TRUE(reducer->Reduce(reduceItems, {outputSegMergeInfo}, false, rsc));
    }

    /* End Parallel Reduce */
    OutputSegmentMergeInfo info;
    info.directory = indexDir;
    reducer->EndParallelReduce({info}, reduceTasks.size(), instResourceVec);

    ++no;
    parallelSegDir = indexDir;
}



IE_NAMESPACE_END(aitheta_plugin);
