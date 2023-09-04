#include <algorithm>
#include <cstddef>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace file_system {
class MunmapPerfTest : public INDEXLIB_TESTBASE
{
public:
    MunmapPerfTest();
    ~MunmapPerfTest();

    DECLARE_CLASS_NAME(MunmapPerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void Write();
    void Read(int* status);
    bool IsFinished() { return _isFinish; }
    void DoMultiThreadTest(size_t readThreadNum, int64_t duration);

private:
    static void MakeData(const std::string& filePath, size_t size);

private:
    std::string _filePath;
    volatile bool _isClosing;
    volatile bool _isFinish;
    volatile bool _isRun;
    util::BlockMemoryQuotaControllerPtr _memoryController;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MunmapPerfTest);

INDEXLIB_UNIT_TEST_CASE(MunmapPerfTest, TestSimpleProcess);

MunmapPerfTest::MunmapPerfTest() {}

MunmapPerfTest::~MunmapPerfTest() {}

void MunmapPerfTest::CaseSetUp()
{
    _filePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "data");
    MakeData(_filePath, 32 * 1024 * 1024);
    _isClosing = false;
    _memoryController = util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MunmapPerfTest::CaseTearDown() {}

void MunmapPerfTest::TestSimpleProcess() { DoMultiThreadTest(1, 2); }

void MunmapPerfTest::Write()
{
    while (!_isRun) {
        usleep(0);
    }

    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, true, false, 1024 * 256, 0);
    sleep(1);
    int64_t maxCloseTime = 0;
    int64_t minCloseTime = 1L << 60;
    while (!IsFinished()) {
        MmapFileNode mmapFile(loadConfig, _memoryController, false);
        ASSERT_EQ(FSEC_OK, mmapFile.Open("LOGICAL_PATH", _filePath, FSOT_MMAP, -1));
        // AUTIL_LOG(ERROR, "before close file");
        int64_t beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
        _isClosing = true;
        ASSERT_EQ(FSEC_OK, mmapFile.Close());
        _isClosing = false;
        int64_t elapsedTime = autil::TimeUtility::currentTimeInMicroSeconds() - beginTime;
        // AUTIL_LOG(ERROR, "after close file");
        maxCloseTime = std::max(maxCloseTime, elapsedTime);
        minCloseTime = std::min(minCloseTime, elapsedTime);
    }
    // AUTIL_LOG(ERROR, "max close time [%ld ms]", maxCloseTime / 1024);
    // AUTIL_LOG(ERROR, "min close time [%ld ms]", minCloseTime / 1024);
}

void MunmapPerfTest::Read(int* status)
{
    while (!_isRun) {
        usleep(0);
    }

    const size_t mallocSize = 1024 * 1024 * 100;
    char* addrs;
    int64_t maxNewTime = 0;
    int64_t minNewTime = 1L << 60;
    int64_t totNewTime = 0;
    int64_t totNewCount = 0;
    while (!IsFinished()) {
        int64_t beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
        addrs = new char[mallocSize];
        int64_t elapsedTime = autil::TimeUtility::currentTimeInMicroSeconds() - beginTime;
        // AUTIL_LOG(ERROR, "new time [%ld us]", elapsedTime);

        maxNewTime = std::max(maxNewTime, elapsedTime);
        minNewTime = std::min(minNewTime, elapsedTime);
        if (_isClosing) {
            totNewTime += elapsedTime;
            totNewCount++;
        }
        beginTime = autil::TimeUtility::currentTimeInMicroSeconds();
        delete[] addrs;
        elapsedTime = autil::TimeUtility::currentTimeInMicroSeconds() - beginTime;
    }
    AUTIL_LOG(ERROR, "max new time [%ld us]", maxNewTime);
    AUTIL_LOG(ERROR, "min new time [%ld us]", minNewTime);
    if (totNewCount)
        AUTIL_LOG(ERROR, "avg new time [%ld us]", totNewTime / totNewCount);
}

void MunmapPerfTest::MakeData(const std::string& filePath, size_t size)
{
    fslib::fs::File* file = fslib::fs::FileSystem::openFile(filePath, fslib::WRITE);
    char* base = new char[size];
    file->write((void*)(base), size);
    file->close();
    delete file;
    delete[] base;
}

void MunmapPerfTest::DoMultiThreadTest(size_t readThreadNum, int64_t duration)
{
    std::vector<int> status(readThreadNum, 0);
    _isFinish = false;
    _isRun = false;

    std::vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < readThreadNum; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread(std::bind(&MunmapPerfTest::Read, this, &status[i]));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(std::bind(&MunmapPerfTest::Write, this));

    _isRun = true;
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < duration) {
        sleep(1);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
    _isFinish = true;

    for (size_t i = 0; i < readThreadNum; ++i) {
        readThreads[i].reset();
    }
    writeThread.reset();
    for (size_t i = 0; i < readThreadNum; ++i) {
        ASSERT_EQ((int)0, status[i]);
    }
}

}} // namespace indexlib::file_system
