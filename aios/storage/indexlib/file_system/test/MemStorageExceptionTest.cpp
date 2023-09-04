#include "gtest/gtest.h"
#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/DirectoryChecker.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/testutil/ExceptionRunner.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace file_system {

class MemStorageExceptionTest : public INDEXLIB_TESTBASE
{
public:
    MemStorageExceptionTest() {}
    ~MemStorageExceptionTest() {}

    DECLARE_CLASS_NAME(MemStorageExceptionTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestSequence();
    void TestSimple();

private:
    void Baseline();

private:
    std::shared_ptr<IFileSystem> _fs;
    std::string _rootDir;
    std::string _savedBaselineDir;

    std::vector<std::function<void()>> _functors;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MemStorageExceptionTest);

INDEXLIB_UNIT_TEST_CASE(MemStorageExceptionTest, TestSequence);
INDEXLIB_UNIT_TEST_CASE(MemStorageExceptionTest, TestSimple);

//////////////////////////////////////////////////////////////////////

void MemStorageExceptionTest::CaseSetUp()
{
    _rootDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "working");
    _savedBaselineDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "saved");
    fslib::fs::FileSystem::_useMock = true;
}

void MemStorageExceptionTest::CaseTearDown() { fslib::fs::FileSystem::_useMock = false; }

namespace {
void DoMultiThreadwork(std::shared_ptr<IFileSystem> fs)
{
    // autil::ThreadPool threadPool(/*threadNum=*/3, /*queueSize=*/100);
    // for (int i = 0; i < 3; ++i)
    // {
    //     threadPool.PushWorkItem(util::makeLambdaWorkItem([&, i]() {
    //         std::shared_ptr<Directory> segDir = Directory::Get(fs)->MakeDirectory("segment_0",
    //         DirectoryOption::Package()); string attrName = "attr_" + StringUtil::toString(i); string content = "data"
    //         + StringUtil::toString(i); segDir->MakeDirectory(attrName)->Store("data", content); segDir->Sync(true);
    //     }));
    // }
    // threadPool.Start("");
    // threadPool.WaitFinish();
    int i = 0;
    std::shared_ptr<Directory> segDir = Directory::Get(fs)->MakeDirectory("segment_0", DirectoryOption::Package());
    string attrName = "attr_" + StringUtil::toString(i);
    string content = "data" + StringUtil::toString(i);
    segDir->MakeDirectory(attrName)->Store("data", content);
    segDir->Sync(true);
}

void Remove(const string& path)
{
    bool isExist = FslibWrapper::IsExist(path).GetOrThrow();
    if (isExist) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::Delete(path, FenceContext::NoFence()).Code());
    }
}

} // namespace

void MemStorageExceptionTest::TestSequence()
{
    ExceptionRunner runner(/*depth=*/3);
    const string& rootDir = _rootDir;
    string instanceRootDir = PathUtil::JoinPath(rootDir, "instance_1/");
    DirectoryChecker::ContentChanger contentChanger {
        {"deploy_meta", nullptr},       {"deploy_index", nullptr},
        {"segment_file_list", nullptr}, {"INDEXLIB_ARCHIVE_FILES", nullptr},
        {"index_summary", nullptr},
    };
    ASSERT_EQ(FslibWrapper::DeleteDir(rootDir, DeleteOption::NoFence(true)).Code(), FSEC_OK);
    ASSERT_EQ(FslibWrapper::DeleteDir(_savedBaselineDir, DeleteOption::NoFence(true)).Code(), FSEC_OK);
    Baseline();
    ASSERT_EQ(FslibWrapper::Copy(rootDir, _savedBaselineDir), FSEC_OK);
    auto baselineFileHashes = DirectoryChecker::GetAllFileHashes(rootDir, contentChanger);
    Remove(rootDir);

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.isOffline = true;
    FileSystemOptions readFsOptions;
    readFsOptions.outputStorage = FSST_MEM;
    readFsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    readFsOptions.isOffline = true;
    std::shared_ptr<IFileSystem> fs = nullptr, readFs = nullptr;
    fs = FileSystemCreator::CreateForWrite("i1", instanceRootDir, fsOptions).GetOrThrow();

    auto reset = [&fs, &readFs, rootDir, &fsOptions, &readFsOptions]() {
        Remove(rootDir);
        if (fs != nullptr) {
            fsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
            std::dynamic_pointer_cast<LogicalFileSystem>(fs)->_entryTable.reset();
            ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));
        }
        if (readFs != nullptr) {
            readFsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
            ASSERT_EQ(FSEC_OK, readFs->Init(readFsOptions));
        }
    };
    runner.AddReset(reset);

    runner.AddAction(Action([&fs]() { DoMultiThreadwork(fs); }));
    runner.AddAction(Action([&fs]() { THROW_IF_FS_ERROR(fs->TEST_Commit(3), ""); }));
    readFs = FileSystemCreator::CreateForWrite("unused", rootDir, readFsOptions).GetOrThrow();
    runner.AddAction(Action([&readFs, instanceRootDir]() {
        THROW_IF_FS_ERROR(readFs->MountVersion(instanceRootDir, 3, "", FSMT_READ_ONLY, nullptr), "");
    }));
    runner.AddAction(Action([&readFs]() { THROW_IF_FS_ERROR(readFs->TEST_Commit(5), ""); }));

    runner.AddVerification([rootDir, contentChanger, baselineFileHashes]() -> bool {
        auto exceptionFileHashes = DirectoryChecker::GetAllFileHashes(rootDir, contentChanger);
        bool res = DirectoryChecker::CheckEqual(baselineFileHashes, exceptionFileHashes);
        INDEXLIB_FATAL_ERROR_IF(!res, FileIO, "Verification failed");
        return res;
    });
    runner.Run();
}

void MemStorageExceptionTest::Baseline()
{
    string instanceRootDir = PathUtil::JoinPath(_rootDir, "instance_1/");

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.isOffline = true;
    auto fs = FileSystemCreator::CreateForWrite("i1", instanceRootDir, fsOptions).GetOrThrow();
    DoMultiThreadwork(fs);
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(3));

    fs = FileSystemCreator::CreateForRead("unused", _rootDir, FileSystemOptions::Offline()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(instanceRootDir, 3, "", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(5));
}

void MemStorageExceptionTest::TestSimple()
{
    fslib::fs::FileSystem::_useMock = true;
    fslib::fs::ExceptionTrigger::InitTrigger(0);
    fslib::fs::ExceptionTrigger::SetExceptionCondition("openFile", "test-file", fslib::EC_NOTSUP);

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.isOffline = true;
    std::shared_ptr<IFileSystem> fs = FileSystemCreator::CreateForWrite("", _rootDir, fsOptions).GetOrThrow();
    WriterOption writerOption;
    std::shared_ptr<FileWriter> writer = fs->CreateFileWriter("test-file", writerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_THROW(fs->Sync(true).GetOrThrow(), util::UnSupportedException);

    fslib::fs::FileSystem::_useMock = false;
}
}} // namespace indexlib::file_system
