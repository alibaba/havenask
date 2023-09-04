#include "gtest/gtest.h"
#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/Timer.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class IFileSystemPerfTest : public INDEXLIB_TESTBASE
{
public:
    IFileSystemPerfTest();
    ~IFileSystemPerfTest();

    DECLARE_CLASS_NAME(IFileSystemPerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckOpenOtherFile();
    void CheckOpenSameFile();

private:
    std::string _readFilePath;
    std::string _otherFile;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, IFileSystemPerfTest);

INDEXLIB_UNIT_TEST_CASE(IFileSystemPerfTest, TestSimpleProcess);

//////////////////////////////////////////////////////////////////////

IFileSystemPerfTest::IFileSystemPerfTest() {}

IFileSystemPerfTest::~IFileSystemPerfTest() {}

void IFileSystemPerfTest::CaseSetUp()
{
    LoadConfigList configList;
    configList.PushBack(LoadConfigListCreator::MakeMmapLoadConfig(true, true, true, 1, 1000));
    FileSystemOptions options;
    options.loadConfigList = configList;
    util::MemoryQuotaControllerPtr quotaControllor(new util::MemoryQuotaController(1024 * 1024 * 1024));
    util::PartitionMemoryQuotaControllerPtr controllor(new util::PartitionMemoryQuotaController(quotaControllor));
    options.memoryQuotaController = controllor;

    _fileSystem = FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                            /*rootPath=*/GET_TEMP_DATA_PATH(), options, util::MetricProviderPtr(),
                                            /*isOverride=*/false)
                      .GetOrThrow();

    _readFilePath = "file";
    Directory::Get(_fileSystem)->Store(_readFilePath, "12345678");
}

void IFileSystemPerfTest::CaseTearDown() {}

void IFileSystemPerfTest::TestSimpleProcess()
{
    vector<ThreadPtr> threads;
    threads.push_back(autil::Thread::createThread(bind(&IFileSystemPerfTest::CheckOpenOtherFile, this)));
    threads.push_back(autil::Thread::createThread(bind(&IFileSystemPerfTest::CheckOpenSameFile, this)));
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    std::shared_ptr<FileReader> fileReader = _fileSystem->CreateFileReader(_readFilePath, FSOT_MMAP).GetOrThrow();
    char* baseAddress = (char*)fileReader->GetBaseAddress();
    ASSERT_TRUE(baseAddress != NULL);
    ASSERT_EQ((size_t)8, fileReader->GetLength());
    ASSERT_EQ('1', *baseAddress);
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    ASSERT_GT((timeLast / 1000000000), (uint64_t)1);
}

void IFileSystemPerfTest::CheckOpenOtherFile()
{
    usleep(10000);
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    string otherFile = "file2";

    WriterOption writerOption;
    std::shared_ptr<FileWriter> fileWriter = _fileSystem->CreateFileWriter(otherFile, writerOption).GetOrThrow();
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    // cout << "open other file time used: " << timeLast << endl;
    ASSERT_LT((timeLast / 1000000000), (uint64_t)10);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
}

void IFileSystemPerfTest::CheckOpenSameFile()
{
    usleep(10000);
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    std::shared_ptr<FileReader> fileReader = _fileSystem->CreateFileReader(_readFilePath, FSOT_MMAP).GetOrThrow();
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    cout << "open same file time used: " << timeLast << endl;
    ASSERT_TRUE((timeLast / 1000000000) >= (uint64_t)7);
}
}} // namespace indexlib::file_system
