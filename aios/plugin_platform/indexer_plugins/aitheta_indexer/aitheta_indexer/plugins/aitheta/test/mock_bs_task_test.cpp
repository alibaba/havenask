#include "aitheta_indexer/plugins/aitheta/test/mock_bs_task_test.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_BEGIN(aitheta_plugin);

void MockBsTaskTest::FullMerge(uint32_t parallelCount) {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    const vector<string> dataPaths = {"testdata/bs_task_mock_test/4doc_8dim.0.txt",
                                      "testdata/bs_task_mock_test/4doc_8dim.1.txt"};
    IndexRetrieverPtr searcher;
    string indexSegPath;
    ASSERT_TRUE(CreateSearcher(params, dataPaths, indexSegPath, searcher, parallelCount));

    vector<std::pair<docid_t, float>> results;
    const string embedding = "1,1,1,1,1,1,1,1";
    {
        string query = "1000#" + embedding + "&n=2";
        ASSERT_TRUE(Search(searcher, query, results));
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].first);
        EXPECT_FLOAT_EQ(7, results[0].second);
    }
    {
        string query = "1007#" + embedding + "&n=2";
        ASSERT_TRUE(Search(searcher, query, results));
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(7, results[0].first);
        EXPECT_FLOAT_EQ(14, results[0].second);
    }
}

void MockBsTaskTest::IncMerge(uint32_t parallelCount) {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    // 全量数据产出
    const vector<string> dataPaths = {"testdata/bs_task_mock_test/4doc_8dim.0.txt",
                                      "testdata/bs_task_mock_test/4doc_8dim.1.txt"};
    string indexSegPath;
    {
        IndexRetrieverPtr searcher;
        ASSERT_TRUE(CreateSearcher(params, dataPaths, indexSegPath, searcher, parallelCount));
    }

    // 实时增量产出
    const vector<string> incDataPath = {"testdata/bs_task_mock_test/2doc_8dim.0.txt",
                                        "testdata/bs_task_mock_test/2doc_8dim.1.txt"};
    vector<string> rawSegPaths(2);
    {
        vector<size_t> docCnts(2);
        ASSERT_TRUE(Build(params, incDataPath[0], rawSegPaths[0], docCnts[0]));
        ASSERT_TRUE(Build(params, incDataPath[1], rawSegPaths[1], docCnts[1]));
    }

    // 全量数据更新
    IndexRetrieverPtr searcher;
    {
        vector<docid_t> baseDocId = {8, 10, 0};
        ASSERT_TRUE(CreateSearcher(params, {rawSegPaths[0], rawSegPaths[1], indexSegPath}, baseDocId, searcher));
        vector<std::pair<docid_t, float>> results;
        const string embedding = "1,1,1,1,1,1,1,1";
        {
            string query = "1000#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(8, results[0].first);
            EXPECT_FLOAT_EQ(7, results[0].second);
        }
        {
            string query = "1002#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(10, results[0].first);
            EXPECT_FLOAT_EQ(9, results[0].second);
        }
        {
            string query = "1007#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(7, results[0].first);
            EXPECT_FLOAT_EQ(14, results[0].second);
        }
    }

    // 实时增量Merge
    string mergedRawSegPath;
    {
        bool isEntireSet = false;
        vector<docid_t> baseDocIds = {0, 2};
        // 删除两条记录
        vector<docid_t> docIdMap = {-1, 1, -1, 3};
        ASSERT_TRUE(Reduce(params, rawSegPaths, isEntireSet, baseDocIds, docIdMap, mergedRawSegPath, parallelCount));
    }

    // 全量数据更新
    IndexRetrieverPtr searcher1;
    {
        vector<docid_t> baseDocId = {0, 8};
        ASSERT_TRUE(CreateSearcher(params, {indexSegPath, mergedRawSegPath}, baseDocId, searcher1));

        vector<std::pair<docid_t, float>> results;
        const string embedding = "1,1,1,1,1,1,1,1";
        {
            string query = "1000#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher1, query, results));
            ASSERT_EQ(0, results.size());
        }
        {
            string query = "1001#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher1, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(9, results[0].first);
            EXPECT_FLOAT_EQ(8, results[0].second);
        }
        {
            string query = "1007#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher1, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(7, results[0].first);
            EXPECT_FLOAT_EQ(14, results[0].second);
        }
    }
}

void MockBsTaskTest::OptimizeMerge(uint32_t parallelCount) {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    // 全量数据产出
    const vector<string> dataPaths = {"testdata/bs_task_mock_test/4doc_8dim.0.txt",
                                      "testdata/bs_task_mock_test/4doc_8dim.1.txt"};
    string indexSegPath;
    {
        IndexRetrieverPtr searcher;
        ASSERT_TRUE(CreateSearcher(params, dataPaths, indexSegPath, searcher, parallelCount));
    }

    // 实时增量产出
    const vector<string> incDataPaths = {"testdata/bs_task_mock_test/2doc_8dim.0.txt",
                                         "testdata/bs_task_mock_test/2doc_8dim.1.txt"};
    vector<string> rawSegPaths(2);
    {
        vector<size_t> docCnts(2);
        ASSERT_TRUE(Build(params, incDataPaths[0], rawSegPaths[0], docCnts[0]));
        ASSERT_TRUE(Build(params, incDataPaths[1], rawSegPaths[1], docCnts[1]));
    }

    // 实时增量Merge
    string mergedRawSegPath;
    {
        bool isEntireSet = false;
        vector<docid_t> baseDocIds = {0, 2};
        // 删除两条记录
        vector<docid_t> docIdMap = {-1, 1, -1, 3};
        ASSERT_TRUE(Reduce(params, rawSegPaths, isEntireSet, baseDocIds, docIdMap, mergedRawSegPath, parallelCount));
    }

    // 实时增量产出 again
    const vector<string> incDataPaths1 = {"testdata/bs_task_mock_test/2doc_8dim.2.txt"};
    string rawSegPath;
    {
        vector<size_t> docCnts(1);
        ASSERT_TRUE(Build(params, incDataPaths1[0], rawSegPath, docCnts[0]));
    }

    // optimize merge
    string mergedIndexSegPath;
    {
        if (parallelCount > 1u) {
            config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
            config::IndexPartitionOptions options;
            util::CounterMapPtr counterMap;
            index_base::PartitionMeta partitionMeta;
            IndexerResourcePtr resource(new IndexerResource(schema, options, counterMap, partitionMeta, "", ""));
            MergeItemHint mergeHint;
            mergeHint.SetDataRatio(0.5f);
            mergeHint.SetId(0);
            MergeTaskResourceVector taskResources;
            AithetaIndexReducer reducer(params);
            reducer.Init(resource, mergeHint, taskResources);
            auto dir = DirectoryCreator::Create(mFileSystem, indexSegPath);
            ASSERT_TRUE(dir != nullptr);
            SegmentMergeInfos segMergeInfos;
            index_base::OutputSegmentMergeInfos outputSegMergeInfos;
            size_t expectedEstimateMemoryUse = InnerGetIndexSegmentLoad4ReduceMemoryUse({dir->GetPath()}, 2);
            EXPECT_EQ(expectedEstimateMemoryUse,
                      reducer.EstimateMemoryUse({dir}, segMergeInfos, outputSegMergeInfos, false));
        }

        bool isEntireSet = true;
        vector<docid_t> baseDocIds = {0, 8, 12};
        vector<docid_t> docIdMap = {-1, -1, -1, -1, 4, 5, 6, 7, -1, -1, -1, 10, 12, 13};

        EXPECT_TRUE(Reduce(params, {indexSegPath, mergedRawSegPath, rawSegPath}, isEntireSet, baseDocIds, docIdMap,
                           mergedIndexSegPath, parallelCount));
        IE_LOG(ERROR, "mergedIndexSegPath[%s]", mergedIndexSegPath.c_str());
        // expect:
        //      old:0 -> new:12
        //      old:1 -> new:13
        //      old:2 -> new:-1
        //      old:3 -> new:10
        //      old:4 -> new:4
        //      old:5 -> new:5
        //      old:6 -> new:6
        //      old:7 -> new:7
    }

    // 全量数据更新
    IndexRetrieverPtr searcher;
    {
        vector<docid_t> baseDocId = {0};
        ASSERT_TRUE(CreateSearcher(params, {mergedIndexSegPath}, baseDocId, searcher));

        vector<std::pair<docid_t, float>> results;
        const string embedding = "1,1,1,1,1,1,1,1";
        {
            string query = "1000#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(12, results[0].first);
        }
        {
            string query = "1002#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(0, results.size());
        }
        {
            string query = "1003#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(10, results[0].first);
        }
        {
            string query = "1007#" + embedding + "&n=2";
            ASSERT_TRUE(Search(searcher, query, results));
            ASSERT_EQ(1, results.size());
            EXPECT_EQ(7, results[0].first);
        }
    }
}

void MockBsTaskTest::MockFullMergeEndParallelReduce() {
    auto rootDir = GET_PARTITION_DIRECTORY();
    auto segDir = rootDir->MakeDirectory("end_parallel_reduce");

    size_t parallelCount = 2u;
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(1000);
        meta.setTotalDocNum(100);
        meta.setValidDocNum(50);
        meta.setMaxCategoryDocNum(20);

        auto dir = segDir->MakeDirectory("inst_2_0");
        ASSERT_TRUE(meta.Dump(dir));

        vector<docid_t> docIdArray = {1, 3, 5};
        vector<int64_t> pkeyArray = {10, 30, 50};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(2000);
        meta.setTotalDocNum(200);
        meta.setValidDocNum(100);
        meta.setMaxCategoryDocNum(40);

        auto dir = segDir->MakeDirectory("inst_2_1");
        ASSERT_TRUE(meta.Dump(dir));
        vector<docid_t> docIdArray = {0, 2, 4};
        vector<int64_t> pkeyArray = {0, 20, 40};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }
    index_base::OutputSegmentMergeInfo mergeInfo;
    mergeInfo.directory = segDir;
    index_base::OutputSegmentMergeInfos mergeInfoVector;
    mergeInfoVector.push_back(mergeInfo);
    AithetaIndexReducer indexReducer({});
    indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

    SegmentMeta meta;
    ASSERT_TRUE(meta.Load(segDir));
    EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
    EXPECT_EQ(40, meta.getMaxCategoryDocNum());
    EXPECT_EQ(300, meta.getTotalDocNum());
    EXPECT_EQ(150, meta.getValidDocNum());
    EXPECT_FALSE(meta.isOptimizeMergeIndex());

    vector<docid_t> docIdArray;
    vector<int64_t> pkeyArray;
    ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

    EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
    EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
}

void MockBsTaskTest::MockOptMergeEndParallelReduce() {
    auto rootDir = GET_PARTITION_DIRECTORY();
    auto segDir = rootDir->MakeDirectory("end_parallel_reduce");

    size_t parallelCount = 2u;
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(1000);
        meta.setTotalDocNum(100);
        meta.setValidDocNum(50);
        meta.setMaxCategoryDocNum(20);
        meta.isOptimizeMergeIndex(true);
        auto dir = segDir->MakeDirectory("inst_2_0");
        ASSERT_TRUE(meta.Dump(dir));

        vector<docid_t> docIdArray = {0, 1, 2, 3, 4, 5};
        vector<int64_t> pkeyArray = {0, 10, 20, 30, 40, 50};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(2000);
        meta.setTotalDocNum(200);
        meta.setValidDocNum(100);
        meta.setMaxCategoryDocNum(40);
        meta.isOptimizeMergeIndex(true);
        auto dir = segDir->MakeDirectory("inst_2_1");
        ASSERT_TRUE(meta.Dump(dir));
    }
    index_base::OutputSegmentMergeInfo mergeInfo;
    mergeInfo.directory = segDir;
    index_base::OutputSegmentMergeInfos mergeInfoVector;
    mergeInfoVector.push_back(mergeInfo);
    AithetaIndexReducer indexReducer({});
    indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

    SegmentMeta meta;
    ASSERT_TRUE(meta.Load(segDir));
    EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
    EXPECT_EQ(40, meta.getMaxCategoryDocNum());
    EXPECT_EQ(300, meta.getTotalDocNum());
    EXPECT_EQ(150, meta.getValidDocNum());
    EXPECT_TRUE(meta.isOptimizeMergeIndex());

    vector<docid_t> docIdArray;
    vector<int64_t> pkeyArray;
    ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

    EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
    EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
}

void MockBsTaskTest::MockIncMergeEndParallelReduce() {
    auto rootDir = GET_PARTITION_DIRECTORY();
    auto segDir = rootDir->MakeDirectory("end_parallel_reduce");

    size_t parallelCount = 1u;

    SegmentMeta meta(SegmentType::kRaw, 0u, 0u);
    ASSERT_TRUE(meta.Dump(segDir));

    index_base::OutputSegmentMergeInfo mergeInfo;
    mergeInfo.directory = segDir;
    index_base::OutputSegmentMergeInfos mergeInfoVector;
    mergeInfoVector.push_back(mergeInfo);
    AithetaIndexReducer indexReducer({});
    indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});
}

void MockBsTaskTest::MockFullMergeEndParallelReduceRetry() {
    auto rootDir = GET_PARTITION_DIRECTORY();
    auto segDir = rootDir->MakeDirectory("end_parallel_reduce");

    size_t parallelCount = 2u;
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(1000);
        meta.setTotalDocNum(100);
        meta.setValidDocNum(50);
        meta.setMaxCategoryDocNum(20);

        auto dir = segDir->MakeDirectory("inst_2_0");
        ASSERT_TRUE(meta.Dump(dir));

        vector<docid_t> docIdArray = {1, 3, 5};
        vector<int64_t> pkeyArray = {10, 30, 50};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(2000);
        meta.setTotalDocNum(200);
        meta.setValidDocNum(100);
        meta.setMaxCategoryDocNum(40);

        auto dir = segDir->MakeDirectory("inst_2_1");
        ASSERT_TRUE(meta.Dump(dir));
        vector<docid_t> docIdArray = {0, 2, 4};
        vector<int64_t> pkeyArray = {0, 20, 40};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }

    {
        // mock EndParallelReduce stage interruption 0
        {
            {
                auto writer = IndexlibIoWrapper::CreateWriter(segDir, DOCID_REMAP_FILE_NAME);
                // damaged content
                vector<docid_t> remapFileContent = {-1, 0, -1, 0};
                writer->Write(remapFileContent.data(), remapFileContent.size());
                writer->Close();
            }
            {
                string incompleteSegMetaContent = R"({"index_type":"index")";
                auto writer = IndexlibIoWrapper::CreateWriter(segDir, SEGMENT_META_FILE_NAME);
                writer->Write(incompleteSegMetaContent.data(), incompleteSegMetaContent.size());
                writer->Close();
            }
        }

        index_base::OutputSegmentMergeInfo mergeInfo;
        mergeInfo.directory = segDir;
        index_base::OutputSegmentMergeInfos mergeInfoVector;
        mergeInfoVector.push_back(mergeInfo);
        AithetaIndexReducer indexReducer({});
        indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

        SegmentMeta meta;
        ASSERT_TRUE(meta.Load(segDir));
        EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
        EXPECT_EQ(40, meta.getMaxCategoryDocNum());
        EXPECT_EQ(300, meta.getTotalDocNum());
        EXPECT_EQ(150, meta.getValidDocNum());
        EXPECT_FALSE(meta.isOptimizeMergeIndex());

        vector<docid_t> docIdArray;
        vector<int64_t> pkeyArray;
        ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

        EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
        EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
    }
    segDir->RemoveFile(SEGMENT_META_FILE_NAME);
    segDir->RemoveFile(DOCID_REMAP_FILE_NAME);
    segDir->RemoveFile("parallel_meta");
    {
        // mock EndParallelReduce stage interruption 1
        {
            auto writer = IndexlibIoWrapper::CreateWriter(segDir, DOCID_REMAP_FILE_NAME);
            // damaged content
            vector<docid_t> remapFileContent = {-1, 0, -1, 0};
            writer->Write(remapFileContent.data(), remapFileContent.size());
            writer->Close();
        }

        index_base::OutputSegmentMergeInfo mergeInfo;
        mergeInfo.directory = segDir;
        index_base::OutputSegmentMergeInfos mergeInfoVector;
        mergeInfoVector.push_back(mergeInfo);
        AithetaIndexReducer indexReducer({});
        indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

        SegmentMeta meta;
        ASSERT_TRUE(meta.Load(segDir));
        EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
        EXPECT_EQ(40, meta.getMaxCategoryDocNum());
        EXPECT_EQ(300, meta.getTotalDocNum());
        EXPECT_EQ(150, meta.getValidDocNum());
        EXPECT_FALSE(meta.isOptimizeMergeIndex());

        vector<docid_t> docIdArray;
        vector<int64_t> pkeyArray;
        ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

        EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
        EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
    }
}

void MockBsTaskTest::MockOptMergeEndParallelReduceRetry() {
    auto rootDir = GET_PARTITION_DIRECTORY();
    auto segDir = rootDir->MakeDirectory("end_parallel_reduce");

    size_t parallelCount = 2u;
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(1000);
        meta.setTotalDocNum(100);
        meta.setValidDocNum(50);
        meta.setMaxCategoryDocNum(20);
        meta.isOptimizeMergeIndex(true);
        auto dir = segDir->MakeDirectory("inst_2_0");
        ASSERT_TRUE(meta.Dump(dir));

        vector<docid_t> docIdArray = {0, 1, 2, 3, 4, 5};
        vector<int64_t> pkeyArray = {0, 10, 20, 30, 40, 50};
        {
            size_t fileSize = 0u;
            IndexSegment::DumpDocIdRemap(dir, docIdArray, pkeyArray, fileSize);
            ASSERT_EQ(docIdArray.size() * sizeof(docid_t) + pkeyArray.size() * sizeof(int64_t), fileSize);
        }
    }
    {
        SegmentMeta meta(SegmentType::kMultiIndex, 0u, 0u);
        meta.setFileSize(2000);
        meta.setTotalDocNum(200);
        meta.setValidDocNum(100);
        meta.setMaxCategoryDocNum(40);
        meta.isOptimizeMergeIndex(true);
        auto dir = segDir->MakeDirectory("inst_2_1");
        ASSERT_TRUE(meta.Dump(dir));
    }

    {
        // mock EndParallelReduce stage interruption 0
        {
            {
                auto writer = IndexlibIoWrapper::CreateWriter(segDir, DOCID_REMAP_FILE_NAME);
                // damaged content
                vector<docid_t> RemapFileContent = {-1, 0, -1, 0};
                writer->Write(RemapFileContent.data(), RemapFileContent.size());
                writer->Close();
            }
            {
                string incompleteSegMetaContent = R"({"index_type":"index")";
                auto writer = IndexlibIoWrapper::CreateWriter(segDir, SEGMENT_META_FILE_NAME);
                writer->Write(incompleteSegMetaContent.data(), incompleteSegMetaContent.size());
                writer->Close();
            }
        }

        index_base::OutputSegmentMergeInfo mergeInfo;
        mergeInfo.directory = segDir;
        index_base::OutputSegmentMergeInfos mergeInfoVector;
        mergeInfoVector.push_back(mergeInfo);
        AithetaIndexReducer indexReducer({});
        indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

        SegmentMeta meta;
        ASSERT_TRUE(meta.Load(segDir));
        EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
        EXPECT_EQ(40, meta.getMaxCategoryDocNum());
        EXPECT_EQ(300, meta.getTotalDocNum());
        EXPECT_EQ(150, meta.getValidDocNum());
        EXPECT_TRUE(meta.isOptimizeMergeIndex());

        vector<docid_t> docIdArray;
        vector<int64_t> pkeyArray;
        ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

        EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
        EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
    }

    segDir->RemoveFile(SEGMENT_META_FILE_NAME);
    segDir->RemoveFile(DOCID_REMAP_FILE_NAME);
    segDir->RemoveFile("parallel_meta");

    {
        // mock EndParallelReduce stage interruption 1
        {
            auto writer = IndexlibIoWrapper::CreateWriter(segDir, DOCID_REMAP_FILE_NAME);
            // damaged content
            vector<docid_t> RemapFileContent = {-1, 0, -1, 0};
            writer->Write(RemapFileContent.data(), RemapFileContent.size());
            writer->Close();
        }

        index_base::OutputSegmentMergeInfo mergeInfo;
        mergeInfo.directory = segDir;
        index_base::OutputSegmentMergeInfos mergeInfoVector;
        mergeInfoVector.push_back(mergeInfo);
        AithetaIndexReducer indexReducer({});
        indexReducer.EndParallelReduce(mergeInfoVector, parallelCount, {});

        SegmentMeta meta;
        ASSERT_TRUE(meta.Load(segDir));
        EXPECT_EQ(3000 + 6 * sizeof(docid_t) + 6 * sizeof(int64_t), meta.getFileSize());
        EXPECT_EQ(40, meta.getMaxCategoryDocNum());
        EXPECT_EQ(300, meta.getTotalDocNum());
        EXPECT_EQ(150, meta.getValidDocNum());
        EXPECT_TRUE(meta.isOptimizeMergeIndex());

        vector<docid_t> docIdArray;
        vector<int64_t> pkeyArray;
        ASSERT_TRUE(IndexSegment::LoadDocIdRemap(segDir, FSOT_MMAP, docIdArray, pkeyArray));

        EXPECT_EQ(vector<docid_t>({0, 1, 2, 3, 4, 5}), docIdArray);
        EXPECT_EQ(vector<int64_t>({0, 10, 20, 30, 40, 50}), pkeyArray);
    }
}

void MockBsTaskTest::MockIncMergeEndParallelReduceRetry() {
    // will not happen
}

void MockBsTaskTest::MockFullMerge() { FullMerge(1); }
void MockBsTaskTest::MockIncMerge() { IncMerge(1); }
void MockBsTaskTest::MockOptimizeMerge() { OptimizeMerge(1); }
void MockBsTaskTest::MockFullMergeWithMultiIndex() { FullMerge(2); }
void MockBsTaskTest::MockIncMergeWithMultiIndex() { IncMerge(2); }
void MockBsTaskTest::MockOptimizeMergeWithMultiIndex() { OptimizeMerge(2); }

IE_NAMESPACE_END(aitheta_plugin);
