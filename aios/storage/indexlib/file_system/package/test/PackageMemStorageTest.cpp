#include "indexlib/file_system/package/PackageMemStorage.h"

#include "gtest/gtest.h"
#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class PackageMemStorageTest : public INDEXLIB_TESTBASE
{
public:
    PackageMemStorageTest();
    ~PackageMemStorageTest();

    DECLARE_CLASS_NAME(PackageMemStorageTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMakeDirectory();
    void TestMakeDirectoryWithPackageFile();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestPackageMemStorage();

private:
    PackageMemStoragePtr CreateStorage(bool asyncFlush = true);

    void CreateAndWriteFile(const std::shared_ptr<Storage>& storage, std::string filePath, std::string expectStr,
                            const WriterOption& option = {});
    std::shared_ptr<FileWriter> CreateFileWriter(const std::shared_ptr<Storage>& storage,
                                                 const std::string& logicalPath, const WriterOption& option = {});
    ErrorCode MakeDirectory(const std::shared_ptr<Storage>& storage, const std::string& path, bool recursive = true,
                            bool isPackage = false);
    void FlushPackage(const std::shared_ptr<Storage>& storage);
    std::future<bool> Sync(const std::shared_ptr<Storage>& storage, bool waitFinish);

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;
    autil::RecursiveThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, PackageMemStorageTest);

INDEXLIB_UNIT_TEST_CASE(PackageMemStorageTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(PackageMemStorageTest, TestMakeDirectoryWithPackageFile);
INDEXLIB_UNIT_TEST_CASE(PackageMemStorageTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(PackageMemStorageTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(PackageMemStorageTest, TestPackageMemStorage);
//////////////////////////////////////////////////////////////////////

PackageMemStorageTest::PackageMemStorageTest() {}

PackageMemStorageTest::~PackageMemStorageTest() {}

void PackageMemStorageTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_TEMP_DATA_PATH());
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void PackageMemStorageTest::CaseTearDown() {}

void PackageMemStorageTest::TestMakeDirectory()
{
    PackageMemStoragePtr storage = CreateStorage();
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "a", true, true));
    ASSERT_EQ(1, storage->_innerDirs.count("a"));
    ASSERT_EQ(0, storage->_innerDirs.count("a/"));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "a/b/c"));

    // ExistException
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "a", true, true));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "a", true, false));
    ASSERT_EQ(FSEC_EXIST, MakeDirectory(storage, "a", false, true));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "a/b/d"));

    FlushPackage(storage);
    Sync(storage, true);
    ASSERT_TRUE(FslibWrapper::IsDir(_rootDir + "/a").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(_rootDir + "/a/package_file.__meta__").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(_rootDir + "/a/package_file.__data__0").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsDir(_rootDir + "/a/b").GetOrThrow());
}

void PackageMemStorageTest::TestRemoveFile()
{
    std::shared_ptr<Storage> storage = CreateStorage(false);
    string expectFlushedStr("1234", 4);
    string fileFlushedPath = PathUtil::JoinPath(_rootDir, "flushed.txt");
    CreateAndWriteFile(storage, "flushed.txt", expectFlushedStr);
    Sync(storage, false);

    string expectNoflushStr("1234", 4);
    string fileNoflushPath = PathUtil::JoinPath(_rootDir, "noflush.txt");
    CreateAndWriteFile(storage, "noflush.txt", expectNoflushStr);

    ASSERT_EQ(FSEC_OK, storage->RemoveFile("flushed.txt", fileFlushedPath, FenceContext::NoFence()));
    ASSERT_FALSE(storage->_fileNodeCache->Find(fileFlushedPath));
}

void PackageMemStorageTest::TestRemoveDirectory()
{
    string inMemDirPath = PathUtil::JoinPath(_rootDir, "inmem");

    {
        PackageMemStoragePtr storage = CreateStorage();
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "inmem", false, true));
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
        Sync(storage, true);
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
        ASSERT_EQ(FSEC_OK, storage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
        // repeat remove dir
        ASSERT_EQ(FSEC_OK, storage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
        ASSERT_TRUE(storage->_innerDirs.empty());
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
    }
    {
        PackageMemStoragePtr storage = CreateStorage();
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "inmem", false, true));
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
        FlushPackage(storage);
        Sync(storage, true);
        ASSERT_TRUE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
        ASSERT_EQ(FSEC_OK, storage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
        ASSERT_TRUE(storage->_innerDirs.empty());
        FlushPackage(storage);
        Sync(storage, true);
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
    }
    {
        PackageMemStoragePtr storage = CreateStorage();
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "inmem", false, true));
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
        ASSERT_EQ(FSEC_OK, storage->RemoveDirectory("inmem", inMemDirPath, FenceContext::NoFence()));
        ASSERT_TRUE(storage->_innerDirs.empty());
        FlushPackage(storage);
        Sync(storage, true);
        ASSERT_FALSE(FslibWrapper::IsExist(inMemDirPath).GetOrThrow());
    }
    {
        PackageMemStoragePtr storage = CreateStorage();
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "inmem", false, true));
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "inmem/subdir", false, false));
        ASSERT_EQ(FSEC_ERROR, storage->RemoveDirectory("inmem", "some-error-path", FenceContext::NoFence()));
        ASSERT_EQ(FSEC_NOTSUP, storage->RemoveDirectory("inmem/subdir", PathUtil::JoinPath(_rootDir, "inmem/subdir"),
                                                        FenceContext::NoFence()));
    }
}

void PackageMemStorageTest::TestPackageMemStorage()
{
    std::shared_ptr<EntryTable> entryTable(new EntryTable("", _rootDir, false));
    PackageMemStoragePtr storage(new PackageMemStorage(false, _memoryController, entryTable));
    FileSystemOptions options;
    options.needFlush = true;
    options.enableAsyncFlush = true;
    ASSERT_EQ(FSEC_OK, storage->Init(std::make_shared<FileSystemOptions>(options)));

    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg0", true, true));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg0/subseg0", true, true));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg0/a", true, false));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg0/b", true, false));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg0/seg1", true, false));
    ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg1", true, false));

    {
        auto it = storage->_innerDirs.begin();
        ASSERT_EQ("seg0", it->first);
        ASSERT_TRUE(it->second.second);

        ++it;
        ASSERT_EQ("seg0/a", it->first);
        ASSERT_TRUE(!it->second.second);

        ++it;
        ASSERT_EQ("seg0/b", it->first);
        ASSERT_TRUE(!it->second.second);

        ++it;
        ASSERT_EQ("seg0/seg1", it->first);
        ASSERT_TRUE(!it->second.second);

        ++it;
        ASSERT_EQ("seg0/subseg0", it->first);
        ASSERT_TRUE(it->second.second);

        ++it;
        ASSERT_EQ("seg1", it->first);
        ASSERT_TRUE(!it->second.second);

        ++it;
        ASSERT_EQ(storage->_innerDirs.end(), it);
    }

    WriterOption option;

    CreateAndWriteFile(storage, "seg0/a.txt", "abcde", option);
    CreateAndWriteFile(storage, "seg0/b.txt", "abcdefghij", option);

    CreateAndWriteFile(storage, "seg0/subseg0/a.txt", "abcde", option);
    CreateAndWriteFile(storage, "seg0/subseg0/b.txt", "abcdefghij", option);

    CreateAndWriteFile(storage, "seg0/a/a.txt", "abcde", option);
    CreateAndWriteFile(storage, "seg0/a/b.txt", "abcdefghij", option);

    CreateAndWriteFile(storage, "seg0/b/a.txt", "abcde", option);
    CreateAndWriteFile(storage, "seg0/b/b.txt", "abcdefghij", option);

    CreateAndWriteFile(storage, "seg0/seg1/a.txt", "abcde", option);
    CreateAndWriteFile(storage, "seg0/seg1/b.txt", "abcdefghij", option);

    {
        auto it = storage->_innerInMemFileMap.begin();
        ASSERT_EQ("seg0/subseg0", it->first);
        auto& list1 = it->second;
        ASSERT_EQ(2u, list1.size());

        auto iter1 = list1.begin();
        ASSERT_EQ("seg0/subseg0/a.txt", iter1->first->GetLogicalPath());

        ++iter1;
        ASSERT_EQ("seg0/subseg0/b.txt", iter1->first->GetLogicalPath());

        ++iter1;
        ASSERT_EQ(list1.end(), iter1);

        ++it;
        ASSERT_EQ("seg0", it->first);
        auto& list2 = it->second;
        ASSERT_EQ(8u, list2.size());

        auto iter2 = list2.begin();
        ASSERT_EQ("seg0/a.txt", iter2->first->GetLogicalPath());

        ++iter2;
        ASSERT_EQ("seg0/b.txt", iter2->first->GetLogicalPath());

        ++iter2;
        ASSERT_EQ("seg0/a/a.txt", iter2->first->GetLogicalPath());

        ++iter2;
        ASSERT_EQ("seg0/a/b.txt", iter2->first->GetLogicalPath());

        ++it;
        ASSERT_EQ(storage->_innerInMemFileMap.end(), it);
    }

    {
        std::string dataFile;
        std::vector<std::pair<std::string, int64_t>> fileOffsets;
        std::vector<std::pair<std::string, int64_t>> metaFileInfos;
        ASSERT_EQ(FSEC_OK, MakeDirectory(storage, "seg2", true, true));
        CreateAndWriteFile(storage, "seg2/c.txt", "abcdefghij", option);
        ASSERT_EQ(FSEC_OK, storage->FlushPackage("seg0"));
        ASSERT_EQ(1, storage->_innerInMemFileMap.size());
        ASSERT_TRUE(storage->_innerInMemFileMap.count("seg2") > 0);
        ASSERT_EQ(1, storage->_innerInMemFileMap.at("seg2").size());

        Sync(storage, true);
        ASSERT_EQ(1, storage->_innerInMemFileMap.size());

        auto entryTable = storage->_entryTable;
        ASSERT_EQ(28682, entryTable->_packageFileLengths[PathUtil::NormalizePath(
                             PathUtil::JoinPath(_rootDir, "seg0/package_file.__data__0"))]);

        std::string content;
        auto ec = FslibWrapper::AtomicLoad(
                      PathUtil::NormalizePath(PathUtil::JoinPath(_rootDir, "seg0/package_file.__meta__")), content)
                      .Code();
        ASSERT_EQ(FSEC_OK, ec);
        PackageFileMeta meta;
        FromJsonString(meta, content);
        ASSERT_EQ(4096, meta.GetFileAlignSize());
        auto iter = meta.Begin();
        ASSERT_EQ("a", iter->_filePath);
        ASSERT_TRUE(iter->_isDir);
        ++iter;
        ASSERT_EQ("b", iter->_filePath);
        ASSERT_TRUE(iter->_isDir);
        ++iter;
        ASSERT_EQ("seg1", iter->_filePath);
        ASSERT_TRUE(iter->_isDir);

        ASSERT_EQ(11, meta._fileMetaVec.size());

        ASSERT_EQ(0, entryTable->GetEntryMeta("seg0/subseg0/a.txt").GetOrThrow().GetOffset());
        ASSERT_EQ(4096, entryTable->GetEntryMeta("seg0/subseg0/b.txt").GetOrThrow().GetOffset());

        ASSERT_EQ(0, entryTable->GetEntryMeta("seg0/a").GetOrThrow().GetOffset());
        ASSERT_EQ("a", meta._fileMetaVec[0]._filePath);
        ASSERT_EQ(0, meta._fileMetaVec[0]._offset);

        ASSERT_EQ(20480, entryTable->GetEntryMeta("seg0/b/b.txt").GetOrThrow().GetOffset());
        ASSERT_EQ("b/b.txt", meta._fileMetaVec[8]._filePath);
        ASSERT_EQ(20480, meta._fileMetaVec[8]._offset);

        ASSERT_EQ(28672, entryTable->GetEntryMeta("seg0/seg1/b.txt").GetOrThrow().GetOffset());
        ASSERT_EQ(10, entryTable->GetEntryMeta("seg0/seg1/b.txt").GetOrThrow().GetLength());
        ASSERT_EQ("seg1/b.txt", meta._fileMetaVec[10]._filePath);
        ASSERT_EQ(28672, meta._fileMetaVec[10]._offset);
        ASSERT_EQ(10, meta._fileMetaVec[10]._length);
    }
}

void PackageMemStorageTest::TestMakeDirectoryWithPackageFile()
{
    // PackageMemStoragePtr storage = CreateStorage(true);
    // storage->MakeDirectory(PathUtil::JoinPath(_rootDir, "a"), true);
    // ASSERT_ANY_THROW(storage->MakeDirectory(
    //                 PathUtil::JoinPath(_rootDir, "a"), false));

    // PackageFileWriterPtr packageFile = storage->CreatePackageFileWriter(
    //         PathUtil::JoinPath(_rootDir, "package_file"));

    // PackageFileUtil::CreatePackageFile(packageFile,
    //         "a/file1:abc#b/file2:abc#/c/file3:abc", "");

    // ASSERT_EQ(FSEC_OK, storage->MakeDirectory(
    //                 PathUtil::JoinPath(_rootDir, "b"), false));

    // FileList fileList;
    // storage->ListDir(_rootDir, fileList, true, true);
    // ASSERT_EQ((size_t)4, fileList.size());
    // ASSERT_EQ(string("a/"), fileList[0]);
    // ASSERT_EQ(string("b/"), fileList[1]);
    // ASSERT_EQ(string("package_file.__data__0"), fileList[2]);
    // ASSERT_EQ(string("package_file.__meta__"), fileList[3]);
}

PackageMemStoragePtr PackageMemStorageTest::CreateStorage(bool asyncFlush)
{
    PackageMemStoragePtr storage(new PackageMemStorage(false, _memoryController, /*entryTable=*/nullptr));
    storage->_fileSystemLock = &_mutex;

    FileSystemOptions options;
    options.needFlush = true;
    options.enableAsyncFlush = asyncFlush;
    EXPECT_EQ(FSEC_OK, storage->Init(std::make_shared<FileSystemOptions>(options)));
    return storage;
}

std::shared_ptr<FileWriter> PackageMemStorageTest::CreateFileWriter(const std::shared_ptr<Storage>& storage,
                                                                    const std::string& logicalPath,
                                                                    const WriterOption& option)
{
    string physicalPath = PathUtil::NormalizePath(PathUtil::JoinPath(_rootDir, logicalPath));
    if (storage->_entryTable) {
        auto ret = storage->_entryTable->CreateFile(logicalPath);
        EXPECT_EQ(FSEC_OK, storage->_entryTable->AddEntryMeta(ret.GetOrThrow()).Code());
    }
    auto ret = storage->CreateFileWriter(logicalPath, physicalPath, option);
    return ret.result;
}

void PackageMemStorageTest::CreateAndWriteFile(const std::shared_ptr<Storage>& storage, string filePath,
                                               string expectStr, const WriterOption& option)
{
    auto memFileWriter = CreateFileWriter(storage, filePath, option);
    ASSERT_EQ(FSEC_OK, memFileWriter->Write(expectStr.data(), expectStr.size()).Code());
    ASSERT_EQ(FSEC_OK, memFileWriter->Close());
}

ErrorCode PackageMemStorageTest::MakeDirectory(const std::shared_ptr<Storage>& storage, const std::string& path,
                                               bool recursive, bool isPackage)
{
    auto physicalPath = PathUtil::JoinPath(_rootDir, path);
    if (storage->_entryTable) {
        std::vector<EntryMeta> metas;
        EXPECT_EQ(FSEC_OK, storage->_entryTable->MakeDirectory(path, recursive, &metas));
        for (auto& meta : metas) {
            EXPECT_EQ(FSEC_OK, storage->_entryTable->AddEntryMeta(meta).Code());
        }
    }
    return storage->MakeDirectory(path, physicalPath, recursive, isPackage);
}

void PackageMemStorageTest::FlushPackage(const std::shared_ptr<Storage>& storage)
{
    THROW_IF_FS_ERROR(storage->FlushPackage(""), "");
}

std::future<bool> PackageMemStorageTest::Sync(const std::shared_ptr<Storage>& storage, bool waitFinish)
{
    auto f = storage->Sync().GetOrThrow();
    if (waitFinish) {
        f.wait();
    }
    return f;
}
}} // namespace indexlib::file_system
