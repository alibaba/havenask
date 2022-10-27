#include <autil/Thread.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/indexlib_file_system_perf_test_unittest.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/util/timer.h"

using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemPerfTest);

IndexlibFileSystemPerfTest::IndexlibFileSystemPerfTest()
{
}

IndexlibFileSystemPerfTest::~IndexlibFileSystemPerfTest()
{
}

void IndexlibFileSystemPerfTest::CaseSetUp()
{
    LoadConfigList configList;
    configList.PushBack(LoadConfigListCreator::MakeMmapLoadConfig(
                    true, true, true, 1, 1000));
    FileSystemOptions options;
    options.loadConfigList = configList;
    mFileSystem.reset(new IndexlibFileSystemImpl(GET_TEST_DATA_PATH()));
    util::MemoryQuotaControllerPtr quotaControllor(
            new util::MemoryQuotaController(1024*1024*1024));
    util::PartitionMemoryQuotaControllerPtr controllor(
            new util::PartitionMemoryQuotaController(quotaControllor));
    options.memoryQuotaController = controllor;
    mFileSystem->Init(options);

    mReadFilePath = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(), "file");
    FileSystemWrapper::AtomicStore(mReadFilePath, "12345678");
}

void IndexlibFileSystemPerfTest::CaseTearDown()
{
}

void IndexlibFileSystemPerfTest::TestSimpleProcess()
{
    vector<ThreadPtr> threads;
    threads.push_back(Thread::createThread(tr1::bind(&IndexlibFileSystemPerfTest::CheckOpenOtherFile, this)));
    threads.push_back(Thread::createThread(tr1::bind(&IndexlibFileSystemPerfTest::CheckOpenSameFile, this)));
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    FileReaderPtr fileReader = mFileSystem->CreateFileReader(
            mReadFilePath, FSOT_MMAP);
    char* baseAddress = (char*)fileReader->GetBaseAddress();
    ASSERT_TRUE(baseAddress != NULL);
    ASSERT_EQ((size_t)8, fileReader->GetLength());
    ASSERT_EQ('1', *baseAddress);
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    cout << "main time used: " << timeLast << endl;
    ASSERT_TRUE((timeLast / 1000000000) >= (uint64_t)8);
}

void IndexlibFileSystemPerfTest::CheckOpenOtherFile()
{
    usleep(10000);
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    string otherFile = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(), "file2");

    FileWriterPtr fileWriter = mFileSystem->CreateFileWriter(otherFile);
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    cout << "open other file time used: " << timeLast << endl;
    ASSERT_TRUE((timeLast / 1000000000) < (uint64_t)4);
    fileWriter->Close();
}

void IndexlibFileSystemPerfTest::CheckOpenSameFile()
{
    usleep(10000);
    uint64_t beginTime = Timer::GetCurrentTimeInNanoSeconds();
    FileReaderPtr fileReader = mFileSystem->CreateFileReader(
            mReadFilePath, FSOT_MMAP);
    uint64_t endTime = Timer::GetCurrentTimeInNanoSeconds();
    uint64_t timeLast = endTime - beginTime;
    cout << "open same file time used: " << timeLast << endl;
    ASSERT_TRUE((timeLast / 1000000000) >= (uint64_t)7);    
}

IE_NAMESPACE_END(file_system);

