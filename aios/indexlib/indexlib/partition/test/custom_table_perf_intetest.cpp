#include "indexlib/partition/test/custom_table_perf_intetest.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/test/table_manager.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/document_parser.h"
#include <cstdio>
#include <autil/Thread.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);


IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomTablePerfTest);

CustomTablePerfTest::CustomTablePerfTest()
{
}

CustomTablePerfTest::~CustomTablePerfTest()
{
}

void CustomTablePerfTest::CaseSetUp()
{
    mNeedForceReopen = false;
}

void CustomTablePerfTest::CaseTearDown()
{
    mTableManager.reset();
}

void CustomTablePerfTest::TestAlwaysForceReopen()
{
    mNeedForceReopen = true;
    TestSimpleProcess();
}

void CustomTablePerfTest::TestSimpleProcess()
{
    string caseDir = GET_TEST_DATA_PATH();
    string indexPluginPath = TEST_DATA_PATH;
    
    string offlineIndexDir =  FileSystemWrapper::JoinPath(
        caseDir, "offline");
    string onlineIndexDir =  FileSystemWrapper::JoinPath(
        caseDir, "online");

    IndexPartitionSchemaPtr schema = index_base::SchemaAdapter::LoadSchema(
            TEST_DATA_PATH,"demo_table_with_param_schema.json");
    IndexPartitionOptions options;
    options.GetBuildConfig(true).maxDocCount = 20;
    options.GetBuildConfig(true).keepVersionCount = 200;
    options.GetBuildConfig(false).keepVersionCount = 200;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;    
    
    mTableManager.reset(new TableManager(
        schema, options, indexPluginPath,
        offlineIndexDir, onlineIndexDir));

    string fullDocString =
        "cmd=add,pk=1,cfield1=1v1,cfield2=1v2;"
        "cmd=add,pk=2,cfield1=2v1,cfield2=2v2;"
        "cmd=add,pk=3,cfield1=3v1,cfield2=3v2;"
        "cmd=add,pk=4,cfield1=4v1,cfield2=4v2;"
        "cmd=add,pk=5,cfield1=5v1,cfield2=5v2;";
    
    ASSERT_TRUE(mTableManager->Init());
    ASSERT_TRUE(mTableManager->BuildFull(fullDocString));

    vector<int> incBatchSize = {5, 1, 10, 20, 9, 11, 8, 7, 33};
    mIncBatchSize.swap(incBatchSize);

    mIncBuildIdx = 0;
    mRtBuildIdx = 0;
    mIncBatchNum = 0;
    mRtBatchNum = 0;

    DoMultiThreadTest(10, 100);
}

string CustomTablePerfTest::CreateRawDocString(int batchIdx, int64_t beginTs, int batchNum)
{
    int docCount = mIncBatchSize[batchIdx];
    char buf[100];
    string rawDocStr;
    int64_t curTs = beginTs;
    for (int pk = 0; pk < docCount; ++pk)
    {
        sprintf(buf, "cmd=add,pk=%d,cfield1=%d,cfield2=%ld,ts=%ld;", pk, batchNum, curTs, curTs);
        rawDocStr += string(buf);
        curTs += 1;
    }
    return rawDocStr;
}

void CustomTablePerfTest::DoBuild(bool isOffline)
{
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    string docStr = CreateRawDocString(mIncBuildIdx, beginTime, mIncBatchNum);

    
    if (isOffline)
    {
        IE_LOG(INFO, "offline build batch[%d]", mIncBatchNum);
        if (mIncBatchNum % 2 == 0)
        {
            if (!mTableManager->BuildInc(docStr, TableManager::OfflineBuildFlag::OFB_NO_DEPLOY))
            {
                IE_LOG(ERROR, "build inc failed");
            }
        }
        else
        {
            if (!mNeedForceReopen)
            {
                if (!mTableManager->BuildInc(docStr))
                {
                    IE_LOG(ERROR, "build inc failed");
                }
            }
            else
            {
                if (!mTableManager->BuildInc(docStr, TableManager::OfflineBuildFlag::OFB_NEED_DEPLOY))
                {
                    IE_LOG(ERROR, "build inc failed");
                }
                if (!mTableManager->ReOpenVersion(INVALID_VERSION, TableManager::ReOpenFlag::RO_FORCE))
                {
                    IE_LOG(ERROR, "reopen latest version failed");
                }
            }
        }
    }
    else
    {
        IE_LOG(INFO, "online build batch[%d]", mIncBatchNum);        
        mTableManager->BuildRt(docStr);
    }
    mIncBatchNum++;
    mIncBuildIdx = (mIncBuildIdx + 1) % mIncBatchSize.size();
}

void CustomTablePerfTest::DoRead(int* status)
{
    int lastBatchIdx = (mIncBatchNum + mIncBatchSize.size() - 1) % mIncBatchSize.size();
    int docCount = mIncBatchSize[lastBatchIdx];
    for (int pk = 0; pk < docCount;  ++pk)
    {
        ResultPtr result = mTableManager->Search(StringUtil::toString(pk));
    }
}

void CustomTablePerfTest::DoMultiThreadTest(
        size_t readThreadNum, int64_t duration)
{
    std::vector<int> status(readThreadNum, 0);
    mIsFinish = false;
    mIsRun = false;
    
    std::vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < readThreadNum; ++i)
    {
        autil::ThreadPtr thread = autil::Thread::createThread(
                std::tr1::bind(&CustomTablePerfTest::Read, this, &status[i]));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr incBuildThread = autil::Thread::createThread(
        std::tr1::bind(&CustomTablePerfTest::Write, this, true));

    autil::ThreadPtr rtBuildThread = autil::Thread::createThread(
        std::tr1::bind(&CustomTablePerfTest::Write, this, false)); 

    mIsRun = true;
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < duration)
    {
        sleep(1);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
    mIsFinish = true;

    for (size_t i = 0; i < readThreadNum; ++i)
    {
        readThreads[i].reset();
    }
    incBuildThread.reset();
    rtBuildThread.reset(); 
    for (size_t i = 0; i < readThreadNum; ++i)
    {
        INDEXLIB_TEST_EQUAL((int)0, status[i]);
    }
}

IE_NAMESPACE_END(partition);

