#include "indexlib/file_system/mmap_file_node.h"
#include "indexlib/file_system/test/munmap_perf_unittest.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/util/path_util.h"
#include <autil/TimeUtility.h>
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MunmapPerfTest);

MunmapPerfTest::MunmapPerfTest()
{
}

MunmapPerfTest::~MunmapPerfTest()
{
}

void MunmapPerfTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mFilePath = PathUtil::JoinPath(mRootDir, "data");
    MakeData(mFilePath, 32*1024*1024);
    mIsClosing = false;
    mMemoryController = util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MunmapPerfTest::CaseTearDown()
{
}

void MunmapPerfTest::TestSimpleProcess()
{
    DoMultiThreadTest(1, 2);
}

void MunmapPerfTest::DoWrite()
{
    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(
            false, true, false, 1024*256, 0);
    sleep(1);
    int64_t maxCloseTime = 0;
    int64_t minCloseTime = 1L << 60;
    while (!IsFinished())
    {
        MmapFileNode mmapFile(loadConfig, mMemoryController, false);
        mmapFile.Open(mFilePath, FSOT_MMAP);
        // IE_LOG(ERROR, "before close file");
        int64_t beginTime = TimeUtility::currentTimeInMicroSeconds();
        mIsClosing = true;
        mmapFile.Close();
        mIsClosing = false;
        int64_t elapsedTime = TimeUtility::currentTimeInMicroSeconds() - beginTime;
        // IE_LOG(ERROR, "after close file");
        maxCloseTime = max(maxCloseTime, elapsedTime);
        minCloseTime = min(minCloseTime, elapsedTime);
    }
    // IE_LOG(ERROR, "max close time [%ld ms]", maxCloseTime / 1024);
    // IE_LOG(ERROR, "min close time [%ld ms]", minCloseTime / 1024);
}

void MunmapPerfTest::DoRead(int* status)
{
    const size_t mallocSize = 1024*1024*100;
    char* addrs;
    int64_t maxNewTime = 0;
    int64_t minNewTime = 1L << 60;
    int64_t totNewTime = 0;
    int64_t totNewCount = 0;
    while (!IsFinished())
    {
        int64_t beginTime = TimeUtility::currentTimeInMicroSeconds();
        addrs = new char[mallocSize];
        int64_t elapsedTime = TimeUtility::currentTimeInMicroSeconds() - beginTime;
        // IE_LOG(ERROR, "new time [%ld us]", elapsedTime);
               
        maxNewTime = max(maxNewTime, elapsedTime);
        minNewTime = min(minNewTime, elapsedTime);
        if (mIsClosing)
        {
            totNewTime += elapsedTime;
            totNewCount++;
        }
        beginTime = TimeUtility::currentTimeInMicroSeconds();
        delete [] addrs;
        elapsedTime = TimeUtility::currentTimeInMicroSeconds() - beginTime;
    }
    IE_LOG(ERROR, "max new time [%ld us]", maxNewTime);
    IE_LOG(ERROR, "min new time [%ld us]", minNewTime);
    if (totNewCount)
        IE_LOG(ERROR, "avg new time [%ld us]", totNewTime / totNewCount);
}

void MunmapPerfTest::MakeData(const string& filePath, size_t size)
{
    File* file = FileSystem::openFile(filePath, WRITE);
    char* base = new char[size];
    file->write((void*)(base), size);
    file->close();
    delete file;
    delete[] base;
}

IE_NAMESPACE_END(file_system);

