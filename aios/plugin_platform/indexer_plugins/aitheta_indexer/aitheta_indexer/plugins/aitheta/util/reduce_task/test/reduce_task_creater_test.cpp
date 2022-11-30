#include "aitheta_indexer/plugins/aitheta/util/reduce_task/test/reduce_task_creater_test.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
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

void ReduceTaskCreaterTest::TestCreateForInc() {
    // prepare data and params
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    const vector<string> dataPaths = {"testdata/reduce_task_creater_test/4doc_8dim.0.txt",
                                      "testdata/reduce_task_creater_test/4doc_8dim.1.txt"};

    // build two raw segments
    vector<string> outputDirs(dataPaths.size());
    for (size_t i = 0; i < dataPaths.size(); ++i) {
        size_t successCnt = 0;
        bool isFullBuildPhase = false;
        ASSERT_TRUE(Build(params, dataPaths[i], outputDirs[i], successCnt, {}, isFullBuildPhase)) << i;
        ASSERT_EQ(4, successCnt) << i;
    }

    uint32_t suggestTaskNum = 2;
    bool isEntireDataSet = false;

    vector<DirectoryPtr> dirVector;
    for (const auto& path : outputDirs) {
        dirVector.push_back(IndexlibIoWrapper::CreateDirectory(path));
    }

    AithetaIndexReducer reducer(params);
    auto reduceTaskVector = reducer.CreateReduceTasks(dirVector, {}, suggestTaskNum, isEntireDataSet, mResMgr);

    ASSERT_EQ(1u, reduceTaskVector.size());
    const auto& reduceTask = reduceTaskVector[0];
    MergeItemHint hint(0, 1.0f);
    MergeTaskResourceVector rscVector;
    rscVector.push_back(mResMgr->GetResource(reduceTask.resourceIds[0]));
    CustomReduceTaskPtr custTask;
    EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
    EXPECT_EQ(std::set<int64_t>({1000l, 1001l, 1002l}), custTask->categorySet);
    EXPECT_EQ(8, custTask->documentCnt);
    EXPECT_EQ(0, custTask->taskId);
    EXPECT_FLOAT_EQ(1.0f, custTask->taskRatio);
}

void ReduceTaskCreaterTest::TestCreateForFull() {
    // prepare data and params
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    const vector<string> dataPaths = {"testdata/reduce_task_creater_test/4doc_8dim.0.txt",
                                      "testdata/reduce_task_creater_test/4doc_8dim.1.txt"};

    // build two raw segments
    vector<string> outputDirs(dataPaths.size());
    for (size_t i = 0; i < dataPaths.size(); ++i) {
        size_t successCnt = 0;
        bool isFullBuildPhase = true;
        ASSERT_TRUE(Build(params, dataPaths[i], outputDirs[i], successCnt, {}, isFullBuildPhase)) << i;
        ASSERT_EQ(4, successCnt) << i;
    }

    vector<DirectoryPtr> dirVector;
    for (const auto& path : outputDirs) {
        dirVector.push_back(IndexlibIoWrapper::CreateDirectory(path));
    }

    uint32_t suggestTaskNum = 2;
    bool isEntireDataSet = true;
    AithetaIndexReducer reducer(params);
    auto reduceTaskVector = reducer.CreateReduceTasks(dirVector, {}, suggestTaskNum, isEntireDataSet, mResMgr);

    ASSERT_EQ(2u, reduceTaskVector.size());
    {
        const auto& reduceTask = reduceTaskVector[0];
        MergeItemHint hint(0, 1.0f);
        MergeTaskResourceVector rscVector;
        rscVector.push_back(mResMgr->GetResource(reduceTask.resourceIds[0]));
        CustomReduceTaskPtr custTask;
        EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
        EXPECT_EQ(std::set<int64_t>({1001l}), custTask->categorySet);
        EXPECT_EQ(4, custTask->documentCnt);
        EXPECT_EQ(0, custTask->taskId);
        EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
    }
    {
        const auto& reduceTask = reduceTaskVector[1];
        MergeItemHint hint(1, 1.0f);
        MergeTaskResourceVector rscVector;
        rscVector.push_back(mResMgr->GetResource(reduceTask.resourceIds[0]));
        CustomReduceTaskPtr custTask;
        EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
        EXPECT_EQ(std::set<int64_t>({1000l, 1002l}), custTask->categorySet);
        EXPECT_EQ(4, custTask->documentCnt);
        EXPECT_EQ(1, custTask->taskId);
        EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
    }
}

void ReduceTaskCreaterTest::TestCreateForOpt() {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    const vector<string> dataPaths = {"testdata/reduce_task_creater_test/4doc_8dim.0.txt",
                                      "testdata/reduce_task_creater_test/4doc_8dim.1.txt"};

    IE_NAMESPACE(index)::IndexRetrieverPtr searcher;
    std::string outputPath;
    uint32_t suggestTaskNum = 2;
    EXPECT_TRUE(CreateSearcher(params, dataPaths, outputPath, searcher, suggestTaskNum));

    bool isEntireDataSet = true;
    AithetaIndexReducer reducer(params);
    auto reduceTaskVector = reducer.CreateReduceTasks({IndexlibIoWrapper::CreateDirectory(outputPath)}, {},
                                                      suggestTaskNum, isEntireDataSet, mResMgr);

    ASSERT_EQ(2, reduceTaskVector.size());
    {
        const auto& task = reduceTaskVector[0];
        EXPECT_EQ(1, task.resourceIds.size());
        MergeItemHint hint(0, 0.5f);
        MergeTaskResourceVector rscVector;
        rscVector.push_back(mResMgr->GetResource(task.resourceIds[0]));
        CustomReduceTaskPtr custTask;
        EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
        EXPECT_EQ(std::set<int64_t>({}), custTask->categorySet);
        EXPECT_EQ(0, custTask->documentCnt);
        EXPECT_EQ(0, custTask->taskId);
        EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
    }
    {
        const auto& task = reduceTaskVector[1];
        EXPECT_EQ(1, task.resourceIds.size());
        MergeItemHint hint(0, 0.5f);
        MergeTaskResourceVector rscVector;
        rscVector.push_back(mResMgr->GetResource(task.resourceIds[0]));
        CustomReduceTaskPtr custTask;
        EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
        EXPECT_EQ(std::set<int64_t>({}), custTask->categorySet);
        EXPECT_EQ(0, custTask->documentCnt);
        EXPECT_EQ(1, custTask->taskId);
        EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
    }
}

void ReduceTaskCreaterTest::TestCreateEmpty() {
    util::KeyValueMap params = {
        {DIMENSION, "8"}, {INDEX_TYPE, INDEX_TYPE_HC}, {BUILD_METRIC_TYPE, L2}, {SEARCH_METRIC_TYPE, INNER_PRODUCT}};

    string outputPath;
    size_t successCnt = 0;
    ASSERT_TRUE(Build(params, std::vector<string>{}, outputPath, successCnt));
    {
        bool isEntireDataSet = true;
        uint32_t suggestTaskNum = 2;
        AithetaIndexReducer reducer(params);
        auto reduceTaskVector = reducer.CreateReduceTasks({IndexlibIoWrapper::CreateDirectory(outputPath)}, {},
                                                          suggestTaskNum, isEntireDataSet, mResMgr);

        ASSERT_EQ(2, reduceTaskVector.size());
        {
            const auto& task = reduceTaskVector[0];
            EXPECT_EQ(1, task.resourceIds.size());
            MergeItemHint hint(0, 0.5f);
            MergeTaskResourceVector rscVector;
            rscVector.push_back(mResMgr->GetResource(task.resourceIds[0]));
            CustomReduceTaskPtr custTask;
            EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
            EXPECT_EQ(std::set<int64_t>({}), custTask->categorySet);
            EXPECT_EQ(0, custTask->documentCnt);
            EXPECT_EQ(0, custTask->taskId);
            EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
        }
        {
            const auto& task = reduceTaskVector[1];
            EXPECT_EQ(1, task.resourceIds.size());
            MergeItemHint hint(0, 0.5f);
            MergeTaskResourceVector rscVector;
            rscVector.push_back(mResMgr->GetResource(task.resourceIds[0]));
            CustomReduceTaskPtr custTask;
            EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
            EXPECT_EQ(std::set<int64_t>({}), custTask->categorySet);
            EXPECT_EQ(0, custTask->documentCnt);
            EXPECT_EQ(1, custTask->taskId);
            EXPECT_FLOAT_EQ(0.5f, custTask->taskRatio);
        }
    }
    {
        bool isEntireDataSet = true;
        uint32_t suggestTaskNum = 1;
        AithetaIndexReducer reducer(params);
        auto reduceTaskVector = reducer.CreateReduceTasks({IndexlibIoWrapper::CreateDirectory(outputPath)}, {},
                                                          suggestTaskNum, isEntireDataSet, mResMgr);

        ASSERT_EQ(1, reduceTaskVector.size());
        {
            const auto& task = reduceTaskVector[0];
            EXPECT_EQ(1, task.resourceIds.size());
            MergeItemHint hint(0, 1.0f);
            MergeTaskResourceVector rscVector;
            rscVector.push_back(mResMgr->GetResource(task.resourceIds[0]));
            CustomReduceTaskPtr custTask;
            EXPECT_EQ(true, ReduceTaskCreater::Retrieve(hint, rscVector, custTask));
            EXPECT_EQ(std::set<int64_t>({}), custTask->categorySet);
            EXPECT_EQ(0, custTask->documentCnt);
            EXPECT_EQ(0, custTask->taskId);
            EXPECT_FLOAT_EQ(1.0f, custTask->taskRatio);
        }
    }
}

IE_NAMESPACE_END(aitheta_plugin);
