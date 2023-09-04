#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class LogicalFileSystemIntetest : public INDEXLIB_TESTBASE
{
public:
    LogicalFileSystemIntetest();
    ~LogicalFileSystemIntetest();

    DECLARE_CLASS_NAME(LogicalFileSystemIntetest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMountFromEntryTable();
    void TestMountFromDeployMeta();
    void TestMountFromVersion();
    void TestMountFromNegativeVersion();
    void TestMountFromLegacyVersion();
    void TestDoMount();
    void TestMountFile();
    void TestMountTypeReadOnly();
    void TestMountTypeReadWrite();
    void TestMountSegment();
    void TestReMount();

    void TestPatch();
    void TestMerge();
    void TestMerge2();
    void TestMergeDiskPackage();
    void TestAfterMergePaths();
    void TestAfterMergePathsWithPackage();
    void TestPackagePathsAfterMerge();
    void TestPackagePathsAfterMergeWithPrefix();

    void TestRecover();
    void TestRecoverAdd2Del();
    void TestRecoverRename();
    void TestRecoverMkdir();
    void TestRecoverMount();
    void TestCommit();

    void TestAdd();
    void TestReadWrite();
    void TestWriteAppendMode();
    void TestCreateOrDeleteEntry();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestMakeDirectory();

    void TestInMemoryFile();
    void TestPackageMemStorageEntryTable();

    void TestLazySimple();
    void TestMergeDir();
    void TestGetEntryMeta();
    void TestCommitSelectedFilesAndDir();
    void TestLinkDirectory();
    void TestLoadAndReadWithLifecycleConfig();
    void TestLoadFileInOutputRoot();
    void TestSegmentDynamicLifecycle();
    void TestMountVersionWithDynamicLifecycle();
    void TestMountVersionWithCheckDiff();

private:
    typedef std::function<std::string(const std::string&)> ParseStringFunc;
    void DoInitTestMount(ParseStringFunc P = [](const std::string& _) { return _; });
    void DoCheckTestMount(
        const std::shared_ptr<LogicalFileSystem>& fs, ParseStringFunc P = [](const std::string& _) { return _; });

private:
    std::shared_ptr<LogicalFileSystem> CreateFS(const std::string& name = "", bool isOffline = true) const;
    void CheckListFile(const std::shared_ptr<LogicalFileSystem>& fs, const std::string& dir, bool isRecursive,
                       std::multiset<std::string> expectList);
    std::string LoadContent(const std::shared_ptr<LogicalFileSystem>& fs, const std::string& path);
    std::multiset<std::string> ListDir(const std::shared_ptr<LogicalFileSystem>& fs, const std::string& dir,
                                       bool isRecursive);

private:
    std::string GetFullPath(const std::string& relativePath);
    std::string GetTestPath(const std::string& path = "") const;
    bool IsExist(const std::string& relativePath);
    bool IsDir(const std::string& relativePath);
    size_t GetFileLength(const std::string& relativePath);
    std::string Load(const std::string& relativePath);
    void MkDir(const std::string& relativePath);
    void Store(const std::string& relativePath, const std::string& content);
    void Remove(const std::string& relativePath);
    void Rename(const std::string& relativeSrcPath, const std::string& relativeDestPath);
    std::vector<std::string> ListDir(const std::string& relativePath, bool recursive = false);

    LogicalFileSystem* CreateFSWithLoadConfig(const std::string& name, bool isOffline,
                                              const LoadConfigList& loadConfigList,
                                              bool redirectPhysicalRoot = false) const;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LogicalFileSystemIntetest);

INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFromEntryTable);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFromDeployMeta);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFromVersion);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFromNegativeVersion);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFromLegacyVersion);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountSegment);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestDoMount);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountFile);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountTypeReadOnly);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountTypeReadWrite);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestReMount);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestPatch);

INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMerge2);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMergeDiskPackage);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestAfterMergePaths);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestAfterMergePathsWithPackage);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestPackagePathsAfterMerge);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestPackagePathsAfterMergeWithPrefix);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRecover);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRecoverAdd2Del);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRecoverRename);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRecoverMkdir);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestCommit);

INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestAdd);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestReadWrite);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestWriteAppendMode);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestCreateOrDeleteEntry);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMakeDirectory);

INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestInMemoryFile);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestPackageMemStorageEntryTable);

INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestLazySimple);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMergeDir);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestGetEntryMeta);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestCommitSelectedFilesAndDir);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestLinkDirectory);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestLoadAndReadWithLifecycleConfig);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestLoadFileInOutputRoot);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestSegmentDynamicLifecycle);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountVersionWithDynamicLifecycle);
INDEXLIB_UNIT_TEST_CASE(LogicalFileSystemIntetest, TestMountVersionWithCheckDiff);

//////////////////////////////////////////////////////////////////////

LogicalFileSystemIntetest::LogicalFileSystemIntetest() {}

LogicalFileSystemIntetest::~LogicalFileSystemIntetest() {}

void LogicalFileSystemIntetest::CaseSetUp() {}

void LogicalFileSystemIntetest::CaseTearDown()
{
    fslib::FileList fileList;
    auto ec = FslibWrapper::ListDirRecursive(GetTestPath(), fileList).Code();
    if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "List path [%s] failed!", GetTestPath().c_str());
    }
    for (const auto& f : fileList) {
        auto ec = FslibWrapper::Delete(f, FenceContext::NoFence()).Code();
        ASSERT_TRUE(ec == FSEC_OK || ec == FSEC_NOENT);
    }
}

std::string LogicalFileSystemIntetest::GetTestPath(const std::string& path) const
{
    return PathUtil::NormalizePath(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), path));
}

std::shared_ptr<LogicalFileSystem> LogicalFileSystemIntetest::CreateFS(const std::string& name, bool isOffline) const
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = isOffline;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem(name, GetTestPath(), nullptr));
    THROW_IF_FS_ERROR(fs->Init(fsOptions), "");
    return fs;
}

void LogicalFileSystemIntetest::TestSimpleProcess()
{
    Store("entry_table.1", R"(
    {
        "files": {
            "dfs://storage/table/g_X/p_0": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "other_path/offset", "length": 19}
            },
            "dfs://storage/table/g_X/p_0/PATCH_ROOT.1": {
                "s_0/attr/age":   {"length": -2},
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age/offset":  {"length": 16}
            }
        }
    })");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", FSMT_READ_ONLY, nullptr));

    CheckListFile(fs, "s_0", false, {"attr"});
    CheckListFile(fs, "s_0", true,
                  {"attr/", "attr/job/", "attr/name/", "attr/name/data", "attr/name/offset", "attr/age/",
                   "attr/age/data", "attr/age/offset"});

    CheckListFile(fs, "s_0/", false, {"attr"});
    CheckListFile(fs, "s_0/", true,
                  {"attr/", "attr/job/", "attr/name/", "attr/name/data", "attr/name/offset", "attr/age/",
                   "attr/age/data", "attr/age/offset"});

    CheckListFile(fs, "", false, {"entry_table.1", "s_0"});
    CheckListFile(fs, "", true,
                  {"s_0/", "s_0/attr/", "s_0/attr/job/", "s_0/attr/name/", "s_0/attr/name/data", "s_0/attr/name/offset",
                   "s_0/attr/age/", "s_0/attr/age/data", "s_0/attr/age/offset"});

    FileList fileList;
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s", ListOption::Recursive(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s", ListOption(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s_0x", ListOption::Recursive(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s_0x", ListOption(), fileList));
}

void LogicalFileSystemIntetest::TestMountFromEntryTable()
{
    SCOPED_TRACE(GET_TEST_NAME());
    DoInitTestMount();
    string entryTableJsonStr = R"( {
        "files": {
            "${TEST_DATA_PATH}": {
                "segment_1_level_0": {"length": -2},
                "segment_2_level_0": {"length": -2},
                "segment_4_level_1": {"length": -2},
                "segment_5_level_1": {"length": -2},
                "segment_1_level_0/segment_info":   {"length": 1},
                "segment_2_level_0/segment_info":   {"length": 1},
                "segment_4_level_1/segment_info":   {"length": 1},
                "segment_5_level_1/segment_info":   {"length": 1}
            }
        }
    })";
    StringUtil::replaceAll(entryTableJsonStr, string("${TEST_DATA_PATH}"), GetTestPath());
    Store("entry_table.7", entryTableJsonStr);

    std::shared_ptr<LogicalFileSystem> fs = CreateFS("", false);
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 7, "prefix", FSMT_READ_ONLY, nullptr));
    // ASSERT_EQ(entryTableJsonStr, LoadContent(fs, "prefix/entry_table.7"));
    DoCheckTestMount(fs);
}

void LogicalFileSystemIntetest::TestMountFromDeployMeta()
{
    SCOPED_TRACE(GET_TEST_NAME());
    DoInitTestMount();
    string versionJsonStr = R"( {
        "segments": [1,2,4,5],
        "versionid": 7
    })";
    string dpMetaJsonStr = R"({
        "deploy_file_metas": [
            { "file_length": -1, "path": "segment_1_level_0/"},
            { "file_length":  1, "path": "segment_1_level_0/segment_info"},
            { "file_length": -1, "path": "segment_2_level_0/"},
            { "file_length":  1, "path": "segment_2_level_0/segment_info"},
            { "file_length": -1, "path": "segment_4_level_1/"},
            { "file_length":  1, "path": "segment_4_level_1/segment_info"},
            { "file_length": -1, "path": "segment_5_level_1/"},
            { "file_length":  1, "path": "segment_5_level_1/segment_info"},
            { "file_length": -1, "path": "deploy_meta.7"}
        ],
        "final_deploy_file_metas": [
            { "file_length": ${VERSION_FILE_LENGTH}, "path": "version.7"}
        ],
        "lifecycle": ""
    } )";
    StringUtil::replaceAll(dpMetaJsonStr, string("${VERSION_FILE_LENGTH}"), std::to_string(versionJsonStr.size()));
    Store("version.7", versionJsonStr);
    Store("deploy_meta.7", dpMetaJsonStr);

    std::shared_ptr<LogicalFileSystem> fs = CreateFS("", false);
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 7, "prefix", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(dpMetaJsonStr, LoadContent(fs, "prefix/deploy_meta.7"));
    ASSERT_EQ(versionJsonStr, LoadContent(fs, "prefix/version.7"));
    DoCheckTestMount(fs);
}

void LogicalFileSystemIntetest::TestMountFromVersion()
{
    SCOPED_TRACE(GET_TEST_NAME());
    DoInitTestMount();
    string versionJsonStr = R"( {
        "segments": [1,2,4,5],
        "versionid": 7,
        "level_info": {
             "level_num" : 2,
             "topology" : "sharding_mod",
             "level_metas" : [ {
                     "level_idx" : 0,
                     "cursor" : 0,
                     "topology" : "hash_mod",
                     "segments" : [ 1, 2 ]
                 }, {
                     "level_idx" : 1,
                     "cursor" : 0,
                     "topology" : "sequence",
                     "segments" : [ 4, 5 ]
                 }
            ]
        }
    })";
    Store("version.7", versionJsonStr);
    Store("schema.json", "");
    Store("index_format_version", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("", false);
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 7, "prefix", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(versionJsonStr, LoadContent(fs, "prefix/version.7"));
    DoCheckTestMount(fs);
}

void LogicalFileSystemIntetest::TestMountFromNegativeVersion()
{
    SCOPED_TRACE(GET_TEST_NAME());
    DoInitTestMount();
    string versionJsonStr = R"( {
        "segments": [1,2,4,5],
        "versionid": 0,
        "level_info": {
             "level_num" : 2,
             "topology" : "sharding_mod",
             "level_metas" : [ {
                     "level_idx" : 0,
                     "cursor" : 0,
                     "topology" : "hash_mod",
                     "segments" : [ 1, 2 ]
                 }, {
                     "level_idx" : 1,
                     "cursor" : 0,
                     "topology" : "sequence",
                     "segments" : [ 4, 5 ]
                 }
            ]
        }
    })";
    Store("version.0", versionJsonStr);
    Store("schema.json", "");
    Store("index_format_version", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("", false);
    ASSERT_EQ(FSEC_NOENT, fs->MountVersion(GetTestPath(), -2, "prefix", FSMT_READ_ONLY, nullptr));
    ASSERT_FALSE(fs->IsExist("prefix/segment_1_level_0/segment_info").GetOrThrow());

    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), -1, "prefix", FSMT_READ_ONLY, nullptr));
    ASSERT_TRUE(fs->IsExist("prefix/segment_1_level_0/segment_info").GetOrThrow());

    DoCheckTestMount(fs);
}

void LogicalFileSystemIntetest::TestMountFromLegacyVersion()
{
    SCOPED_TRACE(GET_TEST_NAME());
    auto TrimSegmentNameLevelInfo = [](const std::string& path) {
        auto frags = StringUtil::split(path, "/");
        for (auto& frag : frags) {
            if (StringUtil::startsWith(frag, "segment_")) {
                frag = frag.substr(0, frag.find("_level_"));
            }
        }
        return StringUtil::toString(frags, "/");
    };
    DoInitTestMount(TrimSegmentNameLevelInfo);
    string versionJsonStr = R"( {
        "segments": [1,2,4,5],
        "versionid": 7
    })";
    Store("version.7", versionJsonStr);
    Store("schema.json", "");
    Store("index_format_version", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("", false);
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 7, "prefix", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(versionJsonStr, LoadContent(fs, "prefix/version.7"));
    DoCheckTestMount(fs, TrimSegmentNameLevelInfo);
}

void LogicalFileSystemIntetest::TestDoMount()
{
    Store("schema.json", "");
    Store("seg_1/f1", "1");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("prefix", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(""), "seg_1", "prefix/seg_1", FSMT_READ_ONLY, true));

    ASSERT_EQ(multiset<string>({"prefix/", "prefix/seg_1/", "prefix/seg_1/f1", "schema.json", "seg_1/", "seg_1/f1"}),
              ListDir(fs, "", true));
    ASSERT_EQ("1", fs->CreateFileReader("prefix/seg_1/f1", FSOT_BUFFERED).GetOrThrow()->TEST_Load());
}

void LogicalFileSystemIntetest::DoInitTestMount(ParseStringFunc P)
{
    Store(P("segment_0_level_0/segment_info"), "0");
    Store(P("segment_1_level_0/segment_info"), "1");
    Store(P("segment_2_level_0/segment_info"), "2");
    Store(P("segment_3_level_0/segment_info"), "3");
    Store(P("segment_4_level_1/segment_info"), "4");
    Store(P("segment_5_level_1/segment_info"), "5");
    Store(P("segment_6_level_2/segment_info"), "6");
}

void LogicalFileSystemIntetest::DoCheckTestMount(const std::shared_ptr<LogicalFileSystem>& fs, ParseStringFunc P)
{
    SCOPED_TRACE("DoCheckTestMount");
    ASSERT_FALSE(fs->IsExist(P("segment_0_level_0")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_0_level_0")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_0_level_0/segment_info")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_3_level_0")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_3_level_0/segment_info")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_6_level_2")).GetOrThrow());
    ASSERT_FALSE(fs->IsExist(P("prefix/segment_6_level_2/segment_info")).GetOrThrow());

    ASSERT_TRUE(fs->IsDir(P("prefix")).GetOrThrow());
    ASSERT_TRUE(fs->IsDir(P("prefix/segment_1_level_0")).GetOrThrow());
    ASSERT_TRUE(fs->IsDir(P("prefix/segment_2_level_0")).GetOrThrow());
    ASSERT_TRUE(fs->IsDir(P("prefix/segment_4_level_1")).GetOrThrow());
    ASSERT_TRUE(fs->IsDir(P("prefix/segment_5_level_1")).GetOrThrow());
    ASSERT_EQ(1, fs->GetFileLength(P("prefix/segment_1_level_0/segment_info")).GetOrThrow());
    ASSERT_EQ(1, fs->GetFileLength(P("prefix/segment_2_level_0/segment_info")).GetOrThrow());
    ASSERT_EQ(1, fs->GetFileLength(P("prefix/segment_4_level_1/segment_info")).GetOrThrow());
    ASSERT_EQ(1, fs->GetFileLength(P("prefix/segment_5_level_1/segment_info")).GetOrThrow());
    ASSERT_EQ("1", LoadContent(fs, P("prefix/segment_1_level_0/segment_info")));
    ASSERT_EQ("2", LoadContent(fs, P("prefix/segment_2_level_0/segment_info")));
    ASSERT_EQ("4", LoadContent(fs, P("prefix/segment_4_level_1/segment_info")));
    ASSERT_EQ("5", LoadContent(fs, P("prefix/segment_5_level_1/segment_info")));
}

void LogicalFileSystemIntetest::TestMountFile()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_FALSE(fs->IsExist("file").GetOrThrow());
    string physicalRoot = GET_PRIVATE_TEST_DATA_PATH();
    ASSERT_EQ(FSEC_OK, fs->MountFile(physicalRoot, "simple.txt", "file", FSMT_READ_ONLY, -1, false));
    ASSERT_TRUE(fs->IsExist("file").GetOrThrow());
    ASSERT_EQ(4, fs->GetFileLength("file").GetOrThrow());
    ASSERT_EQ("test", fs->CreateFileReader("file", FSOT_MEM).GetOrThrow()->TEST_Load());
}

void LogicalFileSystemIntetest::TestMountSegment()
{
    Store("seg_1/data", "abc");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    std::shared_ptr<FileWriter> fileWriter = fs->CreateFileWriter("seg_1/segment_file_list", {}).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MountSegment("seg_1"));
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
}

void LogicalFileSystemIntetest::TestMountTypeReadOnly()
{
    WriterOption writerOption;
    Store("seg_1/seg_info", "");
    Store("seg_1/seg_metrics", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("tmp", false);
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(), "seg_1", "seg_1", FSMT_READ_ONLY, true));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("seg_1/patch.data", writerOption).GetOrThrow()->Close());
    ASSERT_TRUE(IsExist("seg_1/seg_info"));
    ASSERT_TRUE(IsExist("patch.tmp.__fs__/seg_1/"));
    ASSERT_TRUE(IsExist("patch.tmp.__fs__/seg_1/patch.data"));
    ASSERT_TRUE(IsExist("seg_1/"));
    ASSERT_FALSE(IsExist("seg_1/patch.data"));

    ASSERT_EQ(FSEC_OK, fs->RemoveFile("seg_1/seg_info", RemoveOption()));
    ASSERT_FALSE(fs->IsExist("seg_1/seg_info").GetOrThrow());
    ASSERT_TRUE(IsExist("seg_1/seg_info"));
    ASSERT_TRUE(IsExist("patch.tmp.__fs__/seg_1/"));
    ASSERT_TRUE(IsExist("patch.tmp.__fs__/seg_1/patch.data"));

    ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_1/", RemoveOption()));
    ASSERT_FALSE(fs->IsExist("seg_1/").GetOrThrow());
    ASSERT_TRUE(IsExist("seg_1/"));
    ASSERT_TRUE(IsExist("seg_1/seg_metrics"));
    ASSERT_FALSE(IsExist("patch.tmp.__fs__/seg_1/"));
}

void LogicalFileSystemIntetest::TestMountTypeReadWrite()
{
    WriterOption writerOption;
    Store("seg_1/seg_info", "");
    Store("seg_1/seg_metrics", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("tmp");
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(), "", "", FSMT_READ_WRITE, true));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("seg_1/patch.data", writerOption).GetOrThrow()->Close());
    ASSERT_TRUE(IsExist("seg_1/seg_info"));
    ASSERT_FALSE(IsExist("patch.tmp.__fs__/seg_1/"));
    ASSERT_FALSE(IsExist("patch.tmp.__fs__/seg_1/patch.data"));
    ASSERT_TRUE(IsExist("seg_1/"));
    ASSERT_TRUE(IsExist("seg_1/patch.data"));

    ASSERT_EQ(FSEC_OK, fs->RemoveFile("seg_1/seg_info", RemoveOption()));
    ASSERT_FALSE(fs->IsExist("seg_1/seg_info").GetOrThrow());
    ASSERT_FALSE(IsExist("seg_1/seg_info"));
    ASSERT_TRUE(IsExist("seg_1/patch.data"));
    ASSERT_FALSE(IsExist("patch.tmp.__fs__/seg_1/"));

    ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_1/", RemoveOption()));
    ASSERT_FALSE(fs->IsExist("seg_1/").GetOrThrow());
    ASSERT_FALSE(IsExist("seg_1/"));
    ASSERT_FALSE(IsExist("seg_1/seg_metrics"));
    ASSERT_FALSE(IsExist("patch.tmp.__fs__/seg_1/"));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));
}

void LogicalFileSystemIntetest::TestReMount()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("f1", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));
    ASSERT_TRUE(IsExist("f1"));
    ASSERT_TRUE(IsExist("entry_table.0"));

    fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 0, "", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 0, "", FSMT_READ_ONLY, nullptr));
    // fs->Rename("f1", "f2");
    // ASSERT_FALSE(IsExist("f1"));
    // ASSERT_FALSE(fs->IsExist("f1"));
    // ASSERT_TRUE(IsExist("f2"));
    // ASSERT_TRUE(fs->IsExist("f2"));
    // ASSERT_TRUE(IsExist("entry_table.0"));

    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 0, "", FSMT_READ_ONLY, nullptr));
}

void LogicalFileSystemIntetest::TestPatch()
{
    Remove("");
    MkDir("");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("dir", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));
    EXPECT_THAT(ListDir("", true), testing::UnorderedElementsAre("dir/", "entry_table.0"));

    string content = "abcd";
    fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 0, "", FSMT_READ_ONLY, nullptr));
    auto writer = fs->CreateFileWriter("dir/f1", WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Write(content.c_str(), content.size()).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    writer = fs->CreateFileWriter("f2", WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Write(content.c_str(), content.size()).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(1, "", ""));
    EXPECT_THAT(ListDir("", true),
                testing::UnorderedElementsAre("dir/", "entry_table.0", "entry_table.1", "f2", "patch.1.__fs__/",
                                              "patch.1.__fs__/dir/", "patch.1.__fs__/dir/f1"));

    fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", FSMT_READ_ONLY, nullptr));
    ASSERT_EQ(content, fs->CreateFileReader("dir/f1", ReaderOption(FSOT_BUFFERED)).GetOrThrow()->TEST_Load());
    ASSERT_EQ(content, fs->CreateFileReader("f2", ReaderOption(FSOT_BUFFERED)).GetOrThrow()->TEST_Load());
}

void LogicalFileSystemIntetest::TestMerge()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(), "", "", FSMT_READ_WRITE, true));
    Store("instance_0/a0", "");
    Store("instance_0/segment_2/segment_info", "");
    MkDir("instance_0/empty_dir");
    Store("instance_0/z0", "");
    Store("instance_1/a1", "");
    Store("instance_1/segment_2/segment_metrics", "");
    Store("instance_1/z1", "");

    Store("bitmap/a", "");

    ASSERT_TRUE(fs->IsExist("bitmap").GetOrThrow());
    ASSERT_TRUE(fs->IsExist("bitmap/a").GetOrThrow());

    ASSERT_EQ(FSEC_OK, fs->MergeDirs({PathUtil::JoinPath(GetTestPath(), "instance_0/"),
                                      PathUtil::JoinPath(GetTestPath(), "instance_1/")},
                                     "", MergeDirsOption::MergePackage()));

    ASSERT_FALSE(IsExist("instance_0/segment_2/segment_info"));
    ASSERT_FALSE(IsExist("instance_0/segment_2/segment_metrics"));
    ASSERT_TRUE(IsExist("segment_2/segment_info"));
    ASSERT_TRUE(IsExist("segment_2/segment_metrics"));

    ASSERT_TRUE(IsExist("empty_dir"));

    ASSERT_TRUE(IsExist("a0"));
    ASSERT_TRUE(IsExist("a1"));
    ASSERT_TRUE(IsExist("z0"));
    ASSERT_TRUE(IsExist("z1"));
    ASSERT_TRUE(IsExist("bitmap/a"));
}

void LogicalFileSystemIntetest::TestMerge2()
{
    auto fs = FileSystemCreator::CreateForWrite("inst_0", GetTestPath("instance_0")).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("a0", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("segment_2", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_info", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("empty_dir", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("z0", {}).GetOrThrow()->Close());

    fs = FileSystemCreator::CreateForWrite("inst_1", GetTestPath("instance_1")).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("a1", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("segment_2", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_metrics", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("z1", {}).GetOrThrow()->Close());

    fs = FileSystemCreator::CreateForWrite("bitmap", GetTestPath("bitmap")).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("a", {}).GetOrThrow()->Close());

    fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    ASSERT_TRUE(fs->IsExist("bitmap").GetOrThrow());
    ASSERT_TRUE(fs->IsExist("bitmap/a").GetOrThrow());

    ASSERT_EQ(FSEC_OK, fs->MergeDirs({GetFullPath("instance_0"), GetFullPath("instance_1")}, "",
                                     MergeDirsOption::MergePackage()));

    ASSERT_FALSE(IsExist("instance_0/segment_2/segment_info"));
    ASSERT_FALSE(IsExist("instance_0/segment_2/segment_metrics"));
    ASSERT_TRUE(IsExist("segment_2/segment_info"));
    ASSERT_TRUE(IsExist("segment_2/segment_metrics"));

    ASSERT_TRUE(IsExist("empty_dir"));

    ASSERT_TRUE(IsExist("a0"));
    ASSERT_TRUE(IsExist("a1"));
    ASSERT_TRUE(IsExist("z0"));
    ASSERT_TRUE(IsExist("z1"));
    ASSERT_TRUE(IsExist("bitmap/a"));

    // check entry table after merge
    // string unusedPhysicalRoot, relativePhysicalPath;
    // bool inPackage, isDir;
    // EXPECT_TRUE(
    //     fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_metrics",
    //                         unusedPhysicalRoot, relativePhysicalPath, inPackage, isDir));
    // EXPECT_FALSE(inPackage);
    // EXPECT_FALSE(isDir);
    // EXPECT_EQ(relativePhysicalPath, "segment_2/segment_metrics");
    // EXPECT_TRUE(
    //     fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_info",
    //                         unusedPhysicalRoot, relativePhysicalPath, inPackage, isDir));
    // EXPECT_FALSE(inPackage);
    // EXPECT_FALSE(isDir);
    // EXPECT_EQ(relativePhysicalPath, "segment_2/segment_info");
    // EXPECT_TRUE(
    //     fs->TEST_GetPhysicalInfo(/*logicalPath=*/"empty_dir",
    //                         unusedPhysicalRoot, relativePhysicalPath, inPackage, isDir));
    // EXPECT_FALSE(inPackage);
    // EXPECT_TRUE(isDir);
    // EXPECT_EQ(relativePhysicalPath, "empty_dir");
}

void LogicalFileSystemIntetest::TestAfterMergePaths()
{
    auto fs = FileSystemCreator::CreateForWrite("i0t0", GetTestPath("instance_0")).GetOrThrow();
    std::shared_ptr<Directory> dir = Directory::Get(fs);
    std::shared_ptr<Directory> segDir = dir->MakeDirectory("segment_2", DirectoryOption());
    segDir->MakeDirectory("sub_dir1", DirectoryOption());
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_info", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));

    fs = FileSystemCreator::CreateForWrite("i1t0", GetTestPath("instance_1")).GetOrThrow();
    dir = Directory::Get(fs);
    segDir = dir->MakeDirectory("segment_2", DirectoryOption());
    segDir->MakeDirectory("sub_dir2", DirectoryOption());
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_metrics", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));

    fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    EXPECT_EQ(FSEC_OK, fs->MergeDirs({GetTestPath("instance_0/segment_2"), GetTestPath("instance_1/segment_2")},
                                     "segment_2", MergeDirsOption::MergePackage()));

    EXPECT_FALSE(fs->IsExist("instance_0/segment_2/segment_info").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("instance_0/segment_2/sub_dir1").GetOrThrow());
    EXPECT_FALSE(fs->IsExist("instance_1/segment_2/segment_metrics").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("instance_1/segment_2/sub_dir2").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/segment_info").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/segment_metrics").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir1").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir2").GetOrThrow());

    // check entry table after merge
    string unusedPhysicalRoot, relativePhysicalPath;
    bool inPackage, isDir;
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_info", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/segment_info");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_metrics", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/segment_metrics");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir1", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/sub_dir1");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir2", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/sub_dir2");
}

void LogicalFileSystemIntetest::TestAfterMergePathsWithPackage()
{
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    auto fs = FileSystemCreator::CreateForWrite("i0t0", GetTestPath("instance_0"), fsOptions).GetOrThrow();
    std::shared_ptr<Directory> dir = Directory::Get(fs);
    std::shared_ptr<Directory> segDir = dir->MakeDirectory("segment_2", DirectoryOption::Package());
    segDir->MakeDirectory("sub_dir1", DirectoryOption());
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_info", {}).GetOrThrow()->Close());
    ASSERT_TRUE(fs->CommitPackage().OK());

    fs = FileSystemCreator::CreateForWrite("i1t0", GetTestPath("instance_1"), fsOptions).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("segment_2", DirectoryOption::Package()));
    dir = Directory::Get(fs);
    segDir = dir->MakeDirectory("segment_2", DirectoryOption::Package());
    segDir->MakeDirectory("sub_dir2", DirectoryOption());
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_2/segment_metrics", {}).GetOrThrow()->Close());
    ASSERT_TRUE(fs->CommitPackage().OK());

    fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    EXPECT_EQ(FSEC_OK, fs->MergeDirs({GetTestPath("instance_0/segment_2"), GetTestPath("instance_1/segment_2")},
                                     "segment_2", MergeDirsOption::MergePackage()));

    EXPECT_FALSE(fs->IsExist("instance_0/segment_2/segment_info").GetOrThrow());
    EXPECT_FALSE(fs->IsExist("instance_1/segment_2/segment_metrics").GetOrThrow());
    EXPECT_FALSE(fs->IsExist("instance_0/segment_2/sub_dir1").GetOrThrow());
    EXPECT_FALSE(fs->IsExist("instance_1/segment_2/sub_dir2").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/segment_info").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/segment_metrics").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir1").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir2").GetOrThrow());

    // check entry table after merge
    string unusedPhysicalRoot, relativePhysicalPath;
    bool inPackage, isDir;
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_info", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__data__0");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/segment_metrics", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__data__1");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_FALSE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir1", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__meta__");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir2", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__meta__");
}

void LogicalFileSystemIntetest::TestPackagePathsAfterMerge()
{
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    auto fs = FileSystemCreator::CreateForWrite("i0", GetTestPath(""), fsOptions).GetOrThrow();
    std::shared_ptr<Directory> dir = Directory::Get(fs);
    std::shared_ptr<Directory> segDir = dir->MakeDirectory("segment_2", DirectoryOption::Package());
    segDir->MakeDirectory("sub_dir1", DirectoryOption());
    auto subDir2 = segDir->MakeDirectory("sub_dir2", DirectoryOption());
    std::shared_ptr<FileWriter> writer = subDir2->CreateFileWriter("data");
    string output = "example_data";
    ASSERT_EQ(FSEC_OK, writer->Write(output.data(), output.size()).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_TRUE(fs->CommitPackage().OK());

    fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    EXPECT_EQ(FSEC_OK, fs->MergeDirs({GetTestPath("segment_2")}, "segment_2", MergeDirsOption::MergePackage()));

    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir1").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir2").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("segment_2/sub_dir2/data").GetOrThrow());

    // check entry table after merge
    string unusedPhysicalRoot, relativePhysicalPath;
    bool inPackage, isDir;
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir1", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__meta__");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir2", unusedPhysicalRoot, relativePhysicalPath,
                                         inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__meta__");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"segment_2/sub_dir2/data", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "segment_2/package_file.__data__0");
}

void LogicalFileSystemIntetest::TestPackagePathsAfterMergeWithPrefix()
{
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    auto fs = FileSystemCreator::CreateForWrite("i0", GetTestPath(""), fsOptions).GetOrThrow();
    std::shared_ptr<Directory> dir = Directory::Get(fs);
    std::shared_ptr<Directory> segDir = dir->MakeDirectory("segment_2", DirectoryOption::Package());
    segDir->MakeDirectory("sub_dir1", DirectoryOption());
    auto subDir2 = segDir->MakeDirectory("sub_dir2", DirectoryOption());
    std::shared_ptr<FileWriter> writer = subDir2->CreateFileWriter("data");
    string output = "example_data";
    ASSERT_EQ(FSEC_OK, writer->Write(output.data(), output.size()).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_TRUE(fs->CommitPackage().OK());

    fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    EXPECT_EQ(FSEC_OK, fs->MergeDirs({GetTestPath("segment_2")}, "prefix/segment_2", MergeDirsOption::MergePackage()));

    EXPECT_TRUE(fs->IsExist("prefix/segment_2/sub_dir1").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("prefix/segment_2/sub_dir2").GetOrThrow());
    EXPECT_TRUE(fs->IsExist("prefix/segment_2/sub_dir2/data").GetOrThrow());

    // check entry table after merge
    string unusedPhysicalRoot, relativePhysicalPath;
    bool inPackage, isDir;
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"prefix/segment_2/sub_dir1", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "prefix/segment_2/package_file.__meta__");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"prefix/segment_2/sub_dir2", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_TRUE(isDir);
    EXPECT_EQ(relativePhysicalPath, "prefix/segment_2/package_file.__meta__");
    EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"prefix/segment_2/sub_dir2/data", unusedPhysicalRoot,
                                         relativePhysicalPath, inPackage, isDir));
    EXPECT_TRUE(inPackage);
    EXPECT_FALSE(isDir);
    EXPECT_EQ(relativePhysicalPath, "prefix/segment_2/package_file.__data__0");
    EXPECT_TRUE(IsExist("prefix/segment_2/package_file.__meta__"));
    EXPECT_TRUE(IsExist("prefix/segment_2/package_file.__data__0"));
}

void LogicalFileSystemIntetest::TestMergeDiskPackage()
{
    const int instanceCount = 2;
    const int threadCount = 2;
    for (int instanceId = 0; instanceId < instanceCount; ++instanceId) {
        string instanceDirName = "instance_" + StringUtil::toString(instanceId);
        MkDir(instanceDirName);
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_PACKAGE_DISK;
        vector<autil::ThreadPtr> threads;
        for (int threadId = 0; threadId < threadCount; ++threadId) {
            autil::ThreadPtr thread = autil::Thread::createThread(([&, threadId]() {
                auto fs = FileSystemCreator::CreateForWrite("i" + StringUtil::toString(instanceId) + "t" +
                                                                StringUtil::toString(threadId),
                                                            GetTestPath(instanceDirName), fsOptions)
                              .GetOrThrow();
                std::shared_ptr<Directory> dir = Directory::Get(fs);
                string description = "i" + StringUtil::toString(instanceId) + "t" + StringUtil::toString(threadId);
                for (auto segDirName : {"seg_1", "seg_2"}) {
                    std::shared_ptr<Directory> segDir = dir->MakeDirectory(segDirName, DirectoryOption::Package());
                    string attrPath = "attr/name-" + description;

                    std::shared_ptr<Directory> attrDir = segDir->MakeDirectory(attrPath);

                    std::shared_ptr<FileWriter> writer = attrDir->CreateFileWriter("data");
                    string output = description + "-data";
                    ASSERT_EQ(FSEC_OK, writer->Write(output.data(), output.size()).Code());
                    ASSERT_EQ(FSEC_OK, writer->Close());
                    writer = attrDir->CreateFileWriter("offset");
                    output = description + "-offset";
                    ASSERT_EQ(FSEC_OK, writer->Write(output.data(), output.size()).Code());
                    ASSERT_EQ(FSEC_OK, writer->Close());
                }
                ASSERT_TRUE(fs->CommitPackage().OK());
            }));
            threads.push_back(thread);
        }
        threads.clear();
    }
    auto CheckInnerFile = [&](const std::shared_ptr<IFileSystem>& fs) {
        std::shared_ptr<Directory> dir = Directory::Get(fs);
        for (int instanceId = 0; instanceId < instanceCount; ++instanceId) {
            for (int threadId = 0; threadId < threadCount; ++threadId) {
                string description = "i" + StringUtil::toString(instanceId) + "t" + StringUtil::toString(threadId);
                ASSERT_EQ(description + "-data",
                          dir->CreateFileReader("seg_1/attr/name-" + description + "/data", FSOT_MEM)->TEST_Load());
                ASSERT_EQ(description + "-data",
                          dir->CreateFileReader("seg_2/attr/name-" + description + "/data", FSOT_MEM)->TEST_Load());
                ASSERT_EQ(description + "-offset",
                          dir->CreateFileReader("seg_1/attr/name-" + description + "/offset", FSOT_MEM)->TEST_Load());
                ASSERT_EQ(description + "-offset",
                          dir->CreateFileReader("seg_2/attr/name-" + description + "/offset", FSOT_MEM)->TEST_Load());
            }
        }
    };

    // CheckInnerFile(fs);

    auto fs = FileSystemCreator::CreateForRead("test", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    for (auto segDirName : {"seg_1", "seg_2"}) {
        vector<string> inputDirs;
        for (int instanceId = 0; instanceId < instanceCount; ++instanceId) {
            inputDirs.push_back(
                PathUtil::JoinPath(GetTestPath("instance_" + StringUtil::toString(instanceId)), segDirName));
        }
        ASSERT_EQ(FSEC_OK, fs->MergeDirs(inputDirs, segDirName, MergeDirsOption::MergePackage()));
    }
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(3, "", ""));
    EXPECT_THAT(ListDir(""),
                testing::UnorderedElementsAre("seg_1", "seg_2", "entry_table.3", "instance_0", "instance_1"));
    EXPECT_THAT(ListDir("seg_1"), testing::UnorderedElementsAre("package_file.__data__0", "package_file.__data__1",
                                                                "package_file.__data__2", "package_file.__data__3",
                                                                "package_file.__meta__"));

    fs = FileSystemCreator::CreateForRead("merge", GetTestPath(), FileSystemOptions::Offline()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 3, "", FSMT_READ_ONLY, nullptr));
    CheckInnerFile(fs);

    // Check physical paths of package files after merge
    string physicalPath;
    auto CheckEntryTable = [&](const std::shared_ptr<IFileSystem>& fs) {
        string unusedPhysicalRoot, relativePhysicalPath;
        bool inPackage, isDir;
        int dataFileIdx = 0;
        for (int instanceId = 0; instanceId < instanceCount; ++instanceId) {
            for (int threadId = 0; threadId < threadCount; ++threadId) {
                string description = "i" + StringUtil::toString(instanceId) + "t" + StringUtil::toString(threadId);
                EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"seg_1/attr/name-" + description + "/data",
                                                     unusedPhysicalRoot, relativePhysicalPath, inPackage, isDir));
                EXPECT_TRUE(inPackage);
                EXPECT_FALSE(isDir);
                EXPECT_EQ(relativePhysicalPath, "seg_1/package_file.__data__" + StringUtil::toString(dataFileIdx))
                    << description;

                EXPECT_TRUE(fs->TEST_GetPhysicalInfo(/*logicalPath=*/"seg_1/attr/name-" + description,
                                                     unusedPhysicalRoot, relativePhysicalPath, inPackage, isDir));
                EXPECT_TRUE(inPackage);
                EXPECT_TRUE(isDir);
                EXPECT_EQ(relativePhysicalPath, "seg_1/package_file.__meta__") << description;

                dataFileIdx++;
            }
        }
    };
    CheckEntryTable(fs);
}

void LogicalFileSystemIntetest::TestReadWrite()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_TRUE(fs->IsExist("").GetOrThrow());
    ASSERT_FALSE(fs->IsExist("no-exist").GetOrThrow());

    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("a/b/c", DirectoryOption()));
    ASSERT_TRUE(IsExist("a/b"));
    ASSERT_TRUE(IsExist("a"));

    WriterOption writeOption;
    std::shared_ptr<FileWriter> writer = fs->CreateFileWriter("f", writeOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, writer->Write("def", 3).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_EQ(6, fs->GetFileLength("f").GetOrThrow());
    ASSERT_EQ(6, GetFileLength("f"));
    ASSERT_EQ("abcdef", Load("f"));

    char readBuffer[10];
    {
        std::shared_ptr<FileReader> reader = fs->CreateFileReader("f", FSOT_MMAP).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader->Read(readBuffer, 6, 0).Code());
        ASSERT_EQ("abcdef", string(readBuffer, 6));
    }
    {
        auto reader = fs->CreateFileReader("f", FSOT_BUFFERED).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader->Read(readBuffer, 6, 0).Code());
        ASSERT_EQ("abcdef", string(readBuffer, 6));
    }
    {
        auto reader = fs->CreateFileReader("f", FSOT_MEM).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader->Read(readBuffer, 6, 0).Code());
        ASSERT_EQ("abcdef", string(readBuffer, 6));
    }
    // reader = fs->CreateFileReader("f", FSOT_CACHE).GetOrThrow();
    // reader->Read(readBuffer, 6, 0);
    // ASSERT_EQ("abcdef", string(readBuffer, 6));
    {
        auto reader = fs->CreateFileReader("f", FSOT_LOAD_CONFIG).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader->Read(readBuffer, 6, 0).Code());
        ASSERT_EQ("abcdef", string(readBuffer, 6));
    }
    vector<string> fileList;
    ASSERT_EQ(FSEC_OK, fs->ListDir("", ListOption(), fileList));
    EXPECT_THAT(fileList, testing::UnorderedElementsAre("a", "f"));
}

void LogicalFileSystemIntetest::TestWriteAppendMode()
{
    ReaderOption readerOption(FSOT_LOAD_CONFIG);
    WriterOption writerOption;
    WriterOption appendWriterOption;
    appendWriterOption.isAppendMode = true;

    for (bool isFirstCreateWithAppend : {true, false}) {
        TearDown();
        SetUp();

        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        std::shared_ptr<FileWriter> writer =
            fs->CreateFileWriter("f", isFirstCreateWithAppend ? appendWriterOption : writerOption).GetOrThrow();
        ASSERT_EQ(0, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Write("abc", 3).Code());
        ASSERT_EQ(0, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Close());
        ASSERT_EQ(3, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ("abc", Load("f"));
        ASSERT_EQ("abc", fs->CreateFileReader("f", readerOption).GetOrThrow()->TEST_Load());

        writer = fs->CreateFileWriter("f", appendWriterOption).GetOrThrow();
        ASSERT_EQ(3, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Write("def", 3).Code());
        ASSERT_EQ(3, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Close());
        ASSERT_EQ(6, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ("abcdef", Load("f"));
        // ASSERT_EQ("abcdef", fs->CreateFileReader("f", readerOption).GetOrThrow()->TEST_Load());

        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(1, "", ""));

        fs = CreateFS();
        ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", FSMT_READ_ONLY, nullptr));
        ASSERT_EQ("abcdef", fs->CreateFileReader("f", readerOption).GetOrThrow()->TEST_Load());
        writer = fs->CreateFileWriter("f", appendWriterOption).GetOrThrow();
        ASSERT_EQ(6, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Write("ghi", 3).Code());
        ASSERT_EQ(6, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ(FSEC_OK, writer->Close());
        ASSERT_EQ(9, fs->GetFileLength("f").GetOrThrow());
        ASSERT_EQ("abcdefghi", Load("f"));
        // ASSERT_EQ("abcdefghi", fs->CreateFileReader("f", readerOption).GetOrThrow()->TEST_Load());
    }
}

void LogicalFileSystemIntetest::TestRecover()
{
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        ASSERT_EQ(FSEC_OK, fs->MakeDirectory("dir", DirectoryOption()));
        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_TRUE(newFs->IsDir("dir").GetOrThrow());
    }
    tearDown();
    setUp();
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        auto file1 = fs->CreateFileWriter("f1", WriterOption::MemNoDump(5)).GetOrThrow();
        auto writer2 = fs->CreateFileWriter("f2", {}).GetOrThrow();
        auto writer3 = fs->CreateFileWriter("f3", {}).GetOrThrow();
        ASSERT_EQ(FSEC_OK, std::dynamic_pointer_cast<BufferedFileWriter>(writer3)->Flush().Code());

        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_FALSE(newFs->IsExist("f1").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f2").GetOrThrow());
        ASSERT_TRUE(newFs->IsExist("f3").GetOrThrow());

        ASSERT_EQ(FSEC_OK, file1->Close());
        ASSERT_EQ(FSEC_OK, writer2->Close());
        ASSERT_EQ(FSEC_OK, writer3->Close());
    }
}

void LogicalFileSystemIntetest::TestRecoverAdd2Del()
{
    std::shared_ptr<FileWriter> writer2 = nullptr;
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    {
        auto file1 = fs->CreateFileWriter("f1", WriterOption::MemNoDump(5)).GetOrThrow();
        writer2 = fs->CreateFileWriter("f2", {}).GetOrThrow();
        auto writer3 = fs->CreateFileWriter("f3", {}).GetOrThrow();
        ASSERT_EQ(FSEC_OK, file1->Close());
        ASSERT_EQ(FSEC_OK, writer3->Write("1234", 4).Code());
        ASSERT_EQ(FSEC_OK, writer3->Close());
    }
    {
        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_FALSE(newFs->IsExist("f1").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f2").GetOrThrow());
        ASSERT_TRUE(newFs->IsExist("f3").GetOrThrow());
        auto len = newFs->GetFileLength("f3").GetOrThrow();
        ASSERT_EQ(len, 4);
        ASSERT_EQ(FSEC_OK, newFs->RemoveFile("f3", RemoveOption()));
    }
    {
        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_FALSE(newFs->IsExist("f1").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f2").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f3").GetOrThrow());
    }
    ASSERT_EQ(FSEC_OK, writer2->Close());
}

void LogicalFileSystemIntetest::TestRecoverRename()
{
    std::shared_ptr<FileWriter> writer2 = nullptr;
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    {
        auto file1 = fs->CreateFileWriter("f1", WriterOption::MemNoDump(5)).GetOrThrow();
        writer2 = fs->CreateFileWriter("f2", {}).GetOrThrow();
        auto writer3 = fs->CreateFileWriter("f3", {}).GetOrThrow();
        ASSERT_EQ(FSEC_OK, file1->Close());
        ASSERT_EQ(FSEC_OK, writer3->Close());
    }
    {
        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_FALSE(newFs->IsExist("f1").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f2").GetOrThrow());
        ASSERT_TRUE(newFs->IsExist("f3").GetOrThrow());
        ASSERT_EQ(FSEC_OK, newFs->Rename("f3", "f4", FenceContext::NoFence()));
    }
    {
        std::shared_ptr<LogicalFileSystem> newFs = CreateFS();
        ASSERT_FALSE(newFs->IsExist("f1").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f2").GetOrThrow());
        ASSERT_FALSE(newFs->IsExist("f3").GetOrThrow());
        ASSERT_TRUE(newFs->IsExist("f4").GetOrThrow());
        auto len = newFs->GetFileLength("f4").GetOrThrow();
        ASSERT_EQ(len, 0);
    }
    ASSERT_EQ(FSEC_OK, writer2->Close());
}

void LogicalFileSystemIntetest::TestRecoverMkdir()
{
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_0", DirectoryOption()));
    }
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        ASSERT_TRUE(fs->IsDir("seg_0").GetOrThrow());
        ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_0/attr/name", DirectoryOption()));
        ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_0", RemoveOption()));
    }
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFS();
        ASSERT_FALSE(fs->IsDir("seg_0").GetOrThrow());
        ASSERT_FALSE(fs->IsDir("seg_0/attr").GetOrThrow());
        ASSERT_FALSE(fs->IsDir("seg_0/attr/name").GetOrThrow());
    }
}

void LogicalFileSystemIntetest::TestCommit()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS("10");
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_0", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(10, "", ""));
}

void LogicalFileSystemIntetest::TestAdd()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    {
        MkDir("b1");
        auto fs = FileSystemCreator::Create("b1", GetTestPath("b1"), fsOptions).GetOrThrow();
        ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("a", WriterOption()).GetOrThrow()->Close());
        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));
    }
    {
        MkDir("b2");
        auto fs = FileSystemCreator::Create("b2", GetTestPath("b2"), fsOptions).GetOrThrow();
        ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("a", WriterOption()).GetOrThrow()->Close());
        ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));
    }
    //////
    {
        auto fs = CreateFS();
        ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath("b1"), 0, "b1", FSMT_READ_ONLY, nullptr));
        ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath("b2"), 0, "b2", FSMT_READ_ONLY, nullptr));
        auto fileWriter = fs->CreateFileWriter("a", WriterOption::MemNoDump(2)).GetOrThrow();
        ASSERT_EQ(FSEC_OK, fileWriter->Close());
        EXPECT_THROW(fs->CreateFileWriter("b1/a", WriterOption::Slice(2, 2)).GetOrThrow(), ExceptionBase);
        EXPECT_THROW(fs->CreateFileWriter("b2/a", WriterOption::Slice(2, 2)).GetOrThrow(), ExceptionBase);
        ASSERT_EQ(FSEC_EXIST, fs->MakeDirectory("b1/a", DirectoryOption()));
        ASSERT_EQ(FSEC_EXIST, fs->MakeDirectory("b2/a", DirectoryOption()));
        ASSERT_EQ(FSEC_EXIST, fs->MakeDirectory("a", DirectoryOption()));
        ASSERT_EQ(FSEC_NOENT, fs->MountVersion(GetTestPath("b1"), /* not exist*/ 100, "b2", FSMT_READ_ONLY, nullptr));
        ASSERT_EQ(FSEC_NOENT, fs->MountVersion(GetTestPath("b1"), /* not exist*/ 100, "", FSMT_READ_ONLY, nullptr));
        // TODO(qingran)
        // EXPECT_THROW(fs->Mount(GetTestPath("b1"), ""), InconsistentStateException);
    }
}

void LogicalFileSystemIntetest::TestCreateOrDeleteEntry()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("", DirectoryOption()));
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_0", DirectoryOption()));
    ASSERT_TRUE(IsDir("seg_0"));
    ASSERT_EQ(FSEC_NOENT, fs->MakeDirectory("seg_0/attr/name", DirectoryOption::Local(false, false)));
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_0/attr/name", DirectoryOption()));
    ASSERT_TRUE(IsDir("seg_0/attr"));
    ASSERT_TRUE(IsDir("seg_0/attr/name"));

    WriterOption writeOption;
    for (const string& path : {"seg_0/attr/name/data", "seg_0/attr/name/offset", "seg_0/seg_info"}) {
        std::shared_ptr<FileWriter> writer = fs->CreateFileWriter(path, writeOption).GetOrThrow();
        ASSERT_EQ(FSEC_OK, writer->Close());
        ASSERT_TRUE(IsExist(path));
    }

    // test RemoveFile()
    ASSERT_EQ(FSEC_BADARGS, fs->RemoveFile("", RemoveOption()));
    ASSERT_EQ(FSEC_ISDIR, fs->RemoveFile("seg_0", RemoveOption()));
    ASSERT_EQ(FSEC_ISDIR, fs->RemoveFile("seg_0/attr", RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, fs->RemoveFile("non-exist", RemoveOption()));

    ASSERT_EQ(FSEC_OK, fs->RemoveFile("seg_0/attr/name/data", RemoveOption()));
    ASSERT_FALSE(IsExist("seg_0/attr/name/data"));
    ASSERT_TRUE(IsDir("seg_0/attr/name"));
    ASSERT_TRUE(IsExist("seg_0/attr/name/offset"));
    ASSERT_TRUE(IsExist("seg_0/seg_info"));

    // test RemoveDirectory()
    ASSERT_EQ(FSEC_BADARGS, fs->RemoveDirectory("", RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, fs->RemoveDirectory("non-exist", RemoveOption()));
    ASSERT_TRUE(IsExist("seg_0/attr/name"));
    ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_0/attr/name", RemoveOption()));
    ASSERT_FALSE(IsExist("seg_0/attr/name"));
    ASSERT_EQ(FSEC_NOENT, fs->RemoveDirectory("seg_0/attr/name", RemoveOption()));
    ASSERT_TRUE(IsExist("seg_0/attr"));
    ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_0", RemoveOption()));
    ASSERT_FALSE(IsExist("seg_0"));
}

void LogicalFileSystemIntetest::TestRemoveFile()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.needFlush = true;
    fsOptions.enableAsyncFlush = true;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.outputStorage = FSST_MEM;
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem("test", GetTestPath(), nullptr));
    ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));

    Store("seg_1/dat", "");
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetFullPath(""), "seg_1", "seg_1", FSMT_READ_WRITE, true));
    ASSERT_EQ(FSEC_OK, fs->RemoveFile("seg_1/dat", RemoveOption()));
    ASSERT_FALSE(IsExist("seg_1/dat"));
    Store("seg_1/dat", "");
    ASSERT_TRUE(IsExist("seg_1/dat"));
    fs->Sync(true).GetOrThrow();
    ASSERT_TRUE(IsExist("seg_1/dat"));

    auto fileWriter = fs->CreateFileWriter("newf", WriterOption()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("", 0).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_EQ(FSEC_OK, fs->RemoveFile("newf", RemoveOption()));
    ASSERT_FALSE(fs->IsExist("newf").GetOrThrow());
    fileWriter = fs->CreateFileWriter("newf", WriterOption::BufferAtomicDump()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Write("", 0).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_TRUE(fs->IsExist("newf").GetOrThrow());
    ASSERT_TRUE(IsExist("newf"));
}

void LogicalFileSystemIntetest::TestRemoveDirectory()
{
    Store("seg_1/data", "");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_1", RemoveOption()));
    ASSERT_FALSE(IsExist("seg_1"));
    ASSERT_FALSE(IsExist("seg_1/data"));
    ASSERT_FALSE(fs->_entryTable->IsExist("seg_1"));
    ASSERT_FALSE(fs->_entryTable->IsExist("seg_1/data"));

    for (FSStorageType storageType : {FSST_MEM, FSST_DISK, FSST_PACKAGE_MEM /*, FSST_PACKAGE_DISK*/}) {
        tearDown();
        setUp();

        FileSystemOptions fsOptions;
        fsOptions.isOffline = true;
        fsOptions.outputStorage = storageType;
        fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem("", GetTestPath(), nullptr));
        ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));

        ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_1", DirectoryOption::Package()));
        ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("seg_1/data", {}).GetOrThrow()->Close());
        ASSERT_EQ(FSEC_OK, fs->FlushPackage(""));
        fs->Sync(true).GetOrThrow();
        ASSERT_TRUE(IsExist("seg_1"));
        ASSERT_EQ(FSEC_OK, fs->RemoveDirectory("seg_1", RemoveOption()));
        ASSERT_FALSE(fs->IsExist("seg_1").GetOrThrow());
    }
}

void LogicalFileSystemIntetest::TestMakeDirectory()
{
    Store("seg_1/data", "abc");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("seg_1", DirectoryOption()));
    ASSERT_TRUE(fs->IsDir("seg_1").GetOrThrow());
    ASSERT_EQ("abc", fs->CreateFileReader("seg_1/data", FSOT_MEM).GetOrThrow()->TEST_Load());
}

void LogicalFileSystemIntetest::TestInMemoryFile()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem("TestInMemoryFile", GetTestPath(), nullptr));
    ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));

    std::shared_ptr<FileWriter> inMemoryFile = fs->CreateFileWriter("key", WriterOption::Mem(5)).GetOrThrow();
    std::vector<FileInfo> fileInfos;
    ASSERT_EQ(FSEC_OK, fs->ListPhysicalFile("", ListOption::Recursive(true), fileInfos));
    ASSERT_EQ(0, fileInfos.size());

    ASSERT_EQ(FSEC_OK, inMemoryFile->Close());
    ASSERT_EQ(FSEC_OK, fs->ListPhysicalFile("", ListOption::Recursive(true), fileInfos));
    ASSERT_EQ(1, fileInfos.size());
    ASSERT_EQ("key", fileInfos.at(0).filePath);
}

void LogicalFileSystemIntetest::TestPackageMemStorageEntryTable()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_PACKAGE_MEM;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    std::shared_ptr<LogicalFileSystem> fs(
        new LogicalFileSystem("TestPackageMemStorageEntryTable", GetTestPath(), nullptr));
    ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));
    ASSERT_EQ(FSEC_OK, fs->MakeDirectory("segment_0", DirectoryOption::Package()));
    ASSERT_EQ(FSEC_OK, fs->CreateFileWriter("segment_0/data", {}).GetOrThrow()->Close());
    ASSERT_EQ(FSEC_OK, fs->FlushPackage("segment_0"));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0, "", ""));

    std::string packageMetaFilePath = GetFullPath("segment_0/package_file.__meta__");
    ASSERT_EQ(FslibWrapper::GetFileLength(packageMetaFilePath).GetOrThrow(),
              fs->_entryTable->GetPackageFileLength(packageMetaFilePath).GetOrThrow());
    std::string packageDataFilePath = GetFullPath("segment_0/package_file.__data__0");
    ASSERT_EQ(FslibWrapper::GetFileLength(packageDataFilePath).GetOrThrow(),
              fs->_entryTable->GetPackageFileLength(packageDataFilePath).GetOrThrow());
}

void LogicalFileSystemIntetest::CheckListFile(const std::shared_ptr<LogicalFileSystem>& fs, const string& dir,
                                              bool isRecursive, multiset<string> expectList)
{
    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, fs->ListDir(dir, ListOption::Recursive(isRecursive), fileList));
    ASSERT_EQ(expectList, multiset<string>(fileList.begin(), fileList.end()));
}

multiset<string> LogicalFileSystemIntetest::ListDir(const std::shared_ptr<LogicalFileSystem>& fs, const string& dir,
                                                    bool isRecursive)
{
    fslib::FileList fileList;
    EXPECT_EQ(FSEC_OK, fs->ListDir(dir, ListOption::Recursive(isRecursive), fileList));
    return multiset<string>(fileList.begin(), fileList.end());
}

string LogicalFileSystemIntetest::LoadContent(const std::shared_ptr<LogicalFileSystem>& fs, const string& path)
{
    auto reader = fs->CreateFileReader(path, FSOT_MEM).GetOrThrow();
    char* data = (char*)reader->GetBaseAddress();
    string content = string(data, reader->GetLength());
    reader->Close().GetOrThrow();
    return content;
};

/////////////////////////
string LogicalFileSystemIntetest::GetFullPath(const string& relativePath)
{
    return PathUtil::JoinPath(GetTestPath(), relativePath);
}
bool LogicalFileSystemIntetest::IsExist(const string& relativePath)
{
    bool isExist;
    auto ec = FslibWrapper::IsExist(GetFullPath(relativePath), isExist).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: IsExist [%s], ec [%d]", GetFullPath(relativePath).c_str(),
                             ec);
    }
    return isExist;
}
bool LogicalFileSystemIntetest::IsDir(const string& relativePath)
{
    bool isDir = false;
    auto ec = FslibWrapper::IsDir(GetFullPath(relativePath), isDir).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: IsDir [%s], ec [%d]", GetFullPath(relativePath).c_str(), ec);
    }
    return isDir;
}
size_t LogicalFileSystemIntetest::GetFileLength(const string& relativePath)
{
    int64_t fileLength;
    auto ec = FslibWrapper::GetFileLength(GetFullPath(relativePath), fileLength).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: GetFileLength [%s], ec [%d]",
                             GetFullPath(relativePath).c_str(), ec);
    }
    return fileLength;
}
string LogicalFileSystemIntetest::Load(const string& relativePath)
{
    string fileContent;
    auto ec = FslibWrapper::Load(GetFullPath(relativePath), fileContent).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: Load [%s], ec [%d]", GetFullPath(relativePath).c_str(), ec);
    }
    return fileContent;
}
void LogicalFileSystemIntetest::Store(const string& relativePath, const string& content)
{
    MkDir(PathUtil::GetParentDirPath(relativePath));
    auto ec = FslibWrapper::Store(GetFullPath(relativePath), content.c_str(), content.size()).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: Store [%s], ec [%d], length [%lu], content [%s]",
                             GetFullPath(relativePath).c_str(), ec, content.size(), content.c_str());
    }
}
void LogicalFileSystemIntetest::MkDir(const string& relativePath)
{
    auto ec = FslibWrapper::MkDir(GetFullPath(relativePath), true, true).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: Mkdir [%s]", GetFullPath(relativePath).c_str());
    }
}
void LogicalFileSystemIntetest::Remove(const string& relativePath)
{
    if (IsExist(relativePath)) {
        auto ec = FslibWrapper::Delete(GetFullPath(relativePath), FenceContext::NoFence()).Code();
        if (unlikely(FSEC_OK != ec)) {
            INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: Remove [%s], ec [%d]", GetFullPath(relativePath).c_str(),
                                 ec);
        }
    }
}
void LogicalFileSystemIntetest::Rename(const std::string& relativeSrcPath, const std::string& relativeDestPath)
{
    auto ec = FslibWrapper::Rename(GetFullPath(relativeSrcPath), GetFullPath(relativeDestPath)).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: Rename [%s] => [%s], ec [%d]",
                             GetFullPath(relativeSrcPath).c_str(), GetFullPath(relativeDestPath).c_str(), ec);
    }
}
vector<string> LogicalFileSystemIntetest::ListDir(const string& relativePath, bool recursive)
{
    FileList files;
    auto ec = recursive ? FslibWrapper::ListDirRecursive(GetFullPath(relativePath), files).Code()
                        : FslibWrapper::ListDir(GetFullPath(relativePath), files).Code();
    if (unlikely(FSEC_OK != ec)) {
        INDEXLIB_FATAL_ERROR(FileIO, "ut framework error: List [%s], ec [%d]", GetFullPath(relativePath).c_str(), ec);
    }
    return files;
}
/// END TODO

void LogicalFileSystemIntetest::TestLazySimple()
{
    Store("d/f1", "abc");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_TRUE(fs->IsDir("d").GetOrThrow());
    ASSERT_FALSE(fs->IsDir("not-exist").GetOrThrow());
    ASSERT_FALSE(fs->IsDir("not-exist").GetOrThrow());
    ASSERT_TRUE(fs->IsExist("d/f1").GetOrThrow());
    ASSERT_EQ(3, fs->GetFileLength("d/f1").GetOrThrow());
    ASSERT_EQ("abc", fs->CreateFileReader("d/f1", FSOT_MEM).GetOrThrow()->TEST_Load());
    CheckListFile(fs, "", false, {"d"});
    ASSERT_TRUE(fs->_entryTable->GetEntryMeta("d").ec == FSEC_OK);
    CheckListFile(fs, "", true, {"d/", "d/f1"});
    // ASSERT_TRUE(fs->_entryTable->GetEntryMeta("d")->IsLazy());
    CheckListFile(fs, "", false, {"d"});
    CheckListFile(fs, "", true, {"d/", "d/f1"});
}

void LogicalFileSystemIntetest::TestMergeDir()
{
    Store("inst_1/seg_1/f1", "abc-1-1");
    Store("inst_1/seg_2/f1", "abc-2-1");
    Store("inst_2/seg_1/f2", "abc-1-2");
    Store("inst_2/seg_2/f2", "abc-2-2");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->MergeDirs({GetFullPath("inst_1/seg_1"), GetFullPath("inst_2/seg_1")}, "seg_1",
                                     MergeDirsOption::MergePackage()));
    ASSERT_EQ("abc-1-1", Load("seg_1/f1"));
    ASSERT_EQ("abc-1-2", Load("seg_1/f2"));
    ASSERT_EQ("abc-1-1", fs->CreateFileReader("seg_1/f1", FSOT_MEM).GetOrThrow()->TEST_Load());
    ASSERT_EQ("abc-1-2", fs->CreateFileReader("seg_1/f2", FSOT_MEM).GetOrThrow()->TEST_Load());

    ASSERT_EQ(FSEC_OK, fs->MergeDirs({GetFullPath("inst_1/seg_2")}, "seg_2", MergeDirsOption::MergePackage()));
    ASSERT_EQ("abc-2-1", Load("seg_2/f1"));
    ASSERT_EQ("abc-2-1", fs->CreateFileReader("seg_2/f1", FSOT_MEM).GetOrThrow()->TEST_Load());

    ASSERT_EQ(FSEC_OK, fs->MergeDirs({GetFullPath("inst_2/seg_2")}, "seg_2", MergeDirsOption::MergePackage()));
    ASSERT_EQ("abc-2-2", Load("seg_2/f2"));
    ASSERT_EQ("abc-2-2", fs->CreateFileReader("seg_2/f2", FSOT_MEM).GetOrThrow()->TEST_Load());

    Remove("inst_1");
    Remove("inst_2");
    CheckListFile(fs, "", false, {"seg_1", "seg_2"});
    CheckListFile(fs, "", true, {"seg_1/", "seg_1/f1", "seg_1/f2", "seg_2/", "seg_2/f1", "seg_2/f2"});

    // TODO: test merge package
}

void LogicalFileSystemIntetest::TestGetEntryMeta()
{
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();

    Store("seg_1/data", "");
    FSResult<EntryMeta> entryMetaRet = fs->GetEntryMeta("seg_1/data");
    ASSERT_EQ(FSEC_OK, entryMetaRet.ec);
    ASSERT_TRUE(FslibWrapper::IsExist(entryMetaRet.result.GetFullPhysicalPath()).GetOrThrow())
        << entryMetaRet.result.DebugString();

    Store("seg_2/data", "");
    ASSERT_EQ(FSEC_OK, fs->MountDir(GET_TEMP_DATA_PATH(), "seg_2", "seg_2", FSMT_READ_ONLY, true));
    entryMetaRet = fs->GetEntryMeta("seg_2/data");
    ASSERT_EQ(FSEC_OK, entryMetaRet.ec);
    ASSERT_TRUE(FslibWrapper::IsExist(entryMetaRet.result.GetFullPhysicalPath()).GetOrThrow())
        << entryMetaRet.result.DebugString();
    Store("seg_2/data", "");

    Store("seg_3/data", "");
    ASSERT_EQ(FSEC_OK, fs->MountDir(GET_TEMP_DATA_PATH(), "seg_3", "seg_3", FSMT_READ_WRITE, true));
    entryMetaRet = fs->GetEntryMeta("seg_3/data");
    ASSERT_EQ(FSEC_OK, entryMetaRet.ec);
    ASSERT_TRUE(FslibWrapper::IsExist(entryMetaRet.result.GetFullPhysicalPath()).GetOrThrow())
        << entryMetaRet.result.DebugString();
}

void LogicalFileSystemIntetest::TestCommitSelectedFilesAndDir()
{
    Store("seg_1/attr/data", "seg_1/attr/data");
    Store("seg_1/index/data", "seg_1/index/data");
    Store("seg_1/segment_info", "seg_1/segment_info");
    MkDir("seg_1/deletionmap/");
    Store("seg_2/index/data", "seg_2/index/data");
    Store("seg_3/index/data", "seg_3/index/data");
    Store("deploy_meta", "deploy_meta");
    Store("not_in_version_file", "not_in_version_file");

    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    ASSERT_EQ(FSEC_OK, fs->CommitSelectedFilesAndDir(1, {"deploy_meta", "not-exist-file"},
                                                     {"seg_1/index", "seg_1", "seg_2", "not-exit-dir"}, {}, "", "",
                                                     FenceContext::NoFence()));

    ASSERT_TRUE(IsExist("entry_table.1"));
    EntryTableBuilder entryTableBuilder;
    entryTableBuilder.CreateEntryTable("", GET_TEMP_DATA_PATH(), fs->_options);
    ASSERT_EQ(FSEC_OK, entryTableBuilder.FromEntryTableString(Load("entry_table.1"), "",
                                                              PathUtil::NormalizePath(GET_TEMP_DATA_PATH()), false,
                                                              ConflictResolution::OVERWRITE));
    std::vector<EntryMeta> entryMetas = entryTableBuilder._entryTable->ListDir("", true);
    std::vector<std::string> logicalPaths;
    for (const EntryMeta& entryMeta : entryMetas) {
        logicalPaths.push_back(entryMeta.GetLogicalPath());
        if (!entryMeta.IsDir()) {
            ASSERT_EQ(entryMeta.GetLogicalPath(), entryMeta.GetPhysicalPath());
            ASSERT_EQ(entryMeta.GetLogicalPath(), Load(entryMeta.GetPhysicalPath()));
        }
    }
    std::vector<std::string> expectLogicalPaths = {"deploy_meta",
                                                   "seg_1",
                                                   "seg_1/attr",
                                                   "seg_1/attr/data",
                                                   "seg_1/deletionmap",
                                                   "seg_1/index",
                                                   "seg_1/index/data",
                                                   "seg_1/segment_info",
                                                   "seg_2",
                                                   "seg_2/index",
                                                   "seg_2/index/data"};
    EXPECT_THAT(logicalPaths, expectLogicalPaths);

    // TODO: test finalDumpFileName recover
}

void LogicalFileSystemIntetest::TestLinkDirectory()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(3 * 1024 * 1024);
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.needFlush = true;
    fsOptions.useRootLink = true;
    fsOptions.rootLinkWithTs = true;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_GLOBAL_CACHE);
    fsOptions.fileBlockCacheContainer.reset(new FileBlockCacheContainer());
    EXPECT_TRUE(fsOptions.fileBlockCacheContainer->Init("cache_size=2", memoryQuotaController));
    fsOptions.memoryQuotaController.reset(new PartitionMemoryQuotaController(memoryQuotaController));
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem("name", GetTestPath(), nullptr));
    ASSERT_EQ(FSEC_OK, fs->Init(fsOptions));

    WriterOption writerOption;
    std::shared_ptr<FileWriter> writer = fs->CreateFileWriter("f1", writerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Write("aaa", 3).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    fs->Sync(true).GetOrThrow();
    ASSERT_EQ(3, GetFileLength("f1"));
    std::string oldRootLinkPath = fs->GetRootLinkPath();
    ASSERT_EQ("aaa",
              fs->CreateFileReader(PathUtil::JoinPath(oldRootLinkPath, "f1"), FSOT_CACHE).GetOrThrow()->TEST_Load());

    ASSERT_EQ(FSEC_OK, fs->RemoveFile("f1", RemoveOption()));
    sleep(2);
    fs->TEST_SetUseRootLink(true);
    std::string newRootLinkPath = fs->GetRootLinkPath();
    ASSERT_NE(oldRootLinkPath, newRootLinkPath);

    writer = fs->CreateFileWriter("f1", writerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Write("bbbb", 4).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    fs->Sync(true).GetOrThrow();
    ASSERT_EQ(4, GetFileLength("f1"));
    ASSERT_EQ("bbbb",
              fs->CreateFileReader(PathUtil::JoinPath(newRootLinkPath, "f1"), FSOT_CACHE).GetOrThrow()->TEST_Load());
}

LogicalFileSystem* LogicalFileSystemIntetest::CreateFSWithLoadConfig(const std::string& name, bool isOffline,
                                                                     const LoadConfigList& loadConfigList,
                                                                     bool redirectPhysicalRoot) const
{
    auto memQuotaController = indexlib::util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    indexlib::util::PartitionMemoryQuotaControllerPtr partitionQuotaController(
        new indexlib::util::PartitionMemoryQuotaController(memQuotaController));

    indexlib::file_system::FileBlockCacheContainerPtr fileBlockCacheContainer(
        new indexlib::file_system::FileBlockCacheContainer());
    string configStr = "cache_size=200;num_shard_bits=10;io_batch_size=128;";
    if (!fileBlockCacheContainer->Init(configStr, memQuotaController, nullptr, nullptr)) {
        printf("init file block cache failed\n");
        return nullptr;
    }
    FileSystemOptions fsOptions;
    fsOptions.isOffline = isOffline;
    fsOptions.memoryQuotaController = partitionQuotaController;
    fsOptions.loadConfigList = loadConfigList;
    fsOptions.useCache = true;
    fsOptions.fileBlockCacheContainer = fileBlockCacheContainer;
    fsOptions.outputStorage = FSST_MEM;
    fsOptions.redirectPhysicalRoot = redirectPhysicalRoot;

    auto fs = std::make_unique<LogicalFileSystem>(name, GetTestPath(), nullptr);
    THROW_IF_FS_ERROR(fs->Init(fsOptions), "");
    return fs.release();
}

void LogicalFileSystemIntetest::TestLoadAndReadWithLifecycleConfig()
{
    string loadConfigListJson = R"({
        "load_config": [
            {
                "file_patterns": [".*"],
                "load_strategy": "mmap",
                "lifecycle": "hot",
                "remote": true
            },
            {
                "file_patterns": [".*"],
                "load_strategy": "cache",
                "load_strategy_param": {
                   "cache_decompress_file": true,
                   "global_cache": true,
                   "direct_io": false
                },
                "lifecycle": "cold"
            }
        ]
    })";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, loadConfigListJson);
    auto logicalFs = CreateFSWithLoadConfig("test", false, loadConfigList);
    logicalFs->TEST_EnableSupportMmap(false);

    shared_ptr<IFileSystem> fs(logicalFs);
    auto checkFileSystemMetrics = [&fs](FSMetricGroup metricGroup, size_t inputMemFileCount, size_t inputBlockFileCount,
                                        size_t outputMemFileCount, size_t outputBlockFileCount) {
        auto fileSystemMetrics = fs->GetFileSystemMetrics();
        const auto& inputStorageMetrics = fileSystemMetrics.GetInputStorageMetrics();
        const auto& outputStorageMetrics = fileSystemMetrics.GetOutputStorageMetrics();
        ASSERT_EQ(inputMemFileCount, inputStorageMetrics.GetFileCount(metricGroup, FSFT_MEM));
        ASSERT_EQ(inputBlockFileCount, inputStorageMetrics.GetFileCount(metricGroup, FSFT_BLOCK));
        ASSERT_EQ(outputMemFileCount, outputStorageMetrics.GetFileCount(metricGroup, FSFT_MEM));
        ASSERT_EQ(outputBlockFileCount, outputStorageMetrics.GetFileCount(metricGroup, FSFT_BLOCK));
        ASSERT_EQ(inputMemFileCount * 5, inputStorageMetrics.GetFileLength(metricGroup, FSFT_MEM));
        ASSERT_EQ(inputBlockFileCount * 5, inputStorageMetrics.GetFileLength(metricGroup, FSFT_BLOCK));
        ASSERT_EQ(outputMemFileCount * 5, outputStorageMetrics.GetFileLength(metricGroup, FSFT_MEM));
        ASSERT_EQ(outputBlockFileCount * 5, outputStorageMetrics.GetFileLength(metricGroup, FSFT_BLOCK));
    };

    Store("seg_1/f1", "11000");
    Store("seg_1/f2", "12000");
    Store("seg_2/f1", "21000");
    Store("seg_2/f2", "22000");
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(""), "seg_1", "seg_1", FSMT_READ_ONLY, false));
    ASSERT_EQ(FSEC_OK, fs->MountDir(GetTestPath(""), "seg_2", "seg_2", FSMT_READ_ONLY, false));
    checkFileSystemMetrics(FSMG_LOCAL, 0, 0, 0, 0);

    ASSERT_TRUE(fs->SetDirLifecycle("seg_1", "hot"));
    ASSERT_TRUE(fs->SetDirLifecycle("seg_2", "cold"));

    auto ValidateFileType = [&fs](const std::string& filePath, const std::string& fileContent, FSFileType type) {
        cout << "filePath : " << filePath << endl;
        auto ret = fs->CreateFileReader(filePath, FSOT_LOAD_CONFIG);
        ASSERT_TRUE(ret.OK()) << "create fileReader[" << filePath << "] failed";
        auto reader = ret.Value();
        auto fileNode = reader->GetFileNode();
        ASSERT_EQ(type, fileNode->GetType());
        ASSERT_EQ(fileContent, reader->TEST_Load());
    };

    auto GetFileNodeCacheHitNum = [&fs]() -> std::pair<int64_t, int64_t> {
        auto fileSystemMetrics = fs->GetFileSystemMetrics();
        const auto& inputStorageCacheMetrics = fileSystemMetrics.GetInputStorageMetrics().GetFileCacheMetrics();
        const auto& outputStorageCacheMetrics = fileSystemMetrics.GetOutputStorageMetrics().GetFileCacheMetrics();
        return std::make_pair<int64_t, int64_t>(inputStorageCacheMetrics.hitCount.getValue(),
                                                outputStorageCacheMetrics.hitCount.getValue());
    };

    ValidateFileType("seg_1/f1", "11000", FSFT_MEM);
    ValidateFileType("seg_1/f2", "12000", FSFT_MEM);
    checkFileSystemMetrics(FSMG_LOCAL, 2, 0, 0, 0);

    ValidateFileType("seg_2/f1", "21000", FSFT_BLOCK);
    ValidateFileType("seg_2/f2", "22000", FSFT_BLOCK);
    checkFileSystemMetrics(FSMG_LOCAL, 2, 2, 0, 0);

    ASSERT_TRUE(fs->SetDirLifecycle("seg_1", "cold"));
    ASSERT_TRUE(fs->SetDirLifecycle("seg_2", "hot"));

    ValidateFileType("seg_1/f1", "11000", FSFT_BLOCK);
    ValidateFileType("seg_1/f2", "12000", FSFT_BLOCK);
    checkFileSystemMetrics(FSMG_LOCAL, 2, 4, 0, 0);

    ValidateFileType("seg_2/f1", "21000", FSFT_MEM);
    ValidateFileType("seg_2/f2", "22000", FSFT_MEM);
    checkFileSystemMetrics(FSMG_LOCAL, 4, 4, 0, 0);

    DirectoryPtr rootDir = Directory::Get(fs);
    DirectoryPtr seg3Dir = rootDir->MakeDirectory("seg_3", DirectoryOption::Mem());

    auto fileWriter1 = seg3Dir->CreateFileWriter("outfile1");
    auto fileWriter2 = seg3Dir->CreateFileWriter("outfile2");

    ASSERT_EQ(FSEC_OK, fileWriter1->Write("31000", 5).Code());
    ASSERT_EQ(FSEC_OK, fileWriter2->Write("32000", 5).Code());
    ASSERT_EQ(FSEC_OK, fileWriter1->Close());
    ASSERT_EQ(FSEC_OK, fileWriter2->Close());

    checkFileSystemMetrics(FSMG_LOCAL, 4, 4, 2, 0);

    // befor sync, dumped file will be opened in output storage
    auto [inputHit1, outputHit1] = GetFileNodeCacheHitNum();
    ASSERT_TRUE(fs->SetDirLifecycle("seg_3", "cold"));
    ValidateFileType("seg_3/outfile1", "31000", FSFT_MEM);
    ValidateFileType("seg_3/outfile2", "32000", FSFT_MEM);
    checkFileSystemMetrics(FSMG_LOCAL, 4, 4, 2, 0);
    auto [inputHit2, outputHit2] = GetFileNodeCacheHitNum();
    ASSERT_EQ(inputHit2, inputHit1);
    ASSERT_EQ(outputHit2, outputHit1 + 2);

    fs->Sync(true).GetOrThrow();
    ValidateFileType("seg_3/outfile1", "31000", FSFT_BLOCK);
    ValidateFileType("seg_3/outfile2", "32000", FSFT_BLOCK);
    checkFileSystemMetrics(FSMG_LOCAL, 4, 6, 0, 0);

    // after sync, dumped file will be opened in input storage
    auto [inputHit3, outputHit3] = GetFileNodeCacheHitNum();
    ValidateFileType("seg_3/outfile1", "31000", FSFT_BLOCK);
    ValidateFileType("seg_3/outfile2", "32000", FSFT_BLOCK);
    checkFileSystemMetrics(FSMG_LOCAL, 4, 6, 0, 0);
    auto [inputHit4, outputHit4] = GetFileNodeCacheHitNum();
    ASSERT_EQ(inputHit4, inputHit3 + 2);
    ASSERT_EQ(outputHit4, outputHit3);
}

void LogicalFileSystemIntetest::TestLoadFileInOutputRoot()
{
    string loadConfigListJson = R"({
        "load_config": [
            {
                "file_patterns": ["seg_[0-9]+/index.*"],
                "load_strategy": "mmap",
                "lifecycle": "warm"
            },
            {
                "file_patterns": ["seg_[0-9]+/data.*"],
                "load_strategy": "cache",
                "load_strategy_param": {
                   "cache_decompress_file": true,
                   "global_cache": true,
                   "direct_io": false
                },
                "lifecycle": "warm"
            },
            {
                "file_patterns": [".*"],
                "load_strategy": "cache",
                "load_strategy_param": {
                   "cache_decompress_file": true,
                   "global_cache": true,
                   "direct_io": false
                }
            }
        ]
    })";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, loadConfigListJson);

    const auto& loadConfig0 = loadConfigList.GetLoadConfig(0);
    ASSERT_TRUE(loadConfig0.Match("seg_10/indexfile1", "warm"));
    ASSERT_FALSE(loadConfig0.Match("seg_11/indexfile1", ""));

    auto logicalFs = CreateFSWithLoadConfig("test", false, loadConfigList);
    logicalFs->TEST_EnableSupportMmap(false);

    shared_ptr<IFileSystem> fs(logicalFs);
    auto partitionDir = Directory::Get(fs);
    auto dirOnCreate = partitionDir->MakeDirectory("rt_index_partition", DirectoryOption::Mem());
    auto dirOnGet = partitionDir->GetDirectory("rt_index_partition", true);
    // verify a very odd behavior of file system
    ASSERT_TRUE(nullptr != dynamic_cast<MemDirectory*>(dirOnCreate->GetIDirectory().get()));
    ASSERT_TRUE(nullptr != dynamic_cast<LocalDirectory*>(dirOnGet->GetIDirectory().get()));

    auto rtPartitionDir = dirOnGet;

    // test read rt segment before & after sync
    auto seg1Dir = rtPartitionDir->MakeDirectory("seg_1", DirectoryOption::Mem());
    ASSERT_TRUE(nullptr != dynamic_cast<MemDirectory*>(seg1Dir->GetIDirectory().get()));

    auto ValidateFileType = [](const DirectoryPtr& dirPtr, const std::string& filePath, const std::string& fileContent,
                               FSFileType type) {
        auto reader = dirPtr->CreateFileReader(filePath, FSOT_LOAD_CONFIG);
        auto fileNode = reader->GetFileNode();
        ASSERT_EQ(type, fileNode->GetType()) << "filePath: " << filePath << " type unexpected";
        ASSERT_EQ(fileContent, reader->TEST_Load());
    };

    auto fileWriter1 = seg1Dir->CreateFileWriter("outfile1");
    ASSERT_EQ(FSEC_OK, fileWriter1->Write("31000", 5).Code());
    ASSERT_EQ(FSEC_OK, fileWriter1->Close());
    ValidateFileType(seg1Dir, "outfile1", "31000", FSFT_MEM);

    auto seg1DirOnRead = rtPartitionDir->GetDirectory("seg_1", false);
    ASSERT_TRUE(seg1DirOnRead);
    ASSERT_TRUE(nullptr != dynamic_cast<LocalDirectory*>(seg1DirOnRead->GetIDirectory().get()));
    ValidateFileType(seg1DirOnRead, "outfile1", "31000", FSFT_MEM);
    fs->Sync(true).GetOrThrow();
    ValidateFileType(seg1DirOnRead, "outfile1", "31000", FSFT_BLOCK);

    // test modify rt segment lifecycle
    auto seg2Dir = rtPartitionDir->MakeDirectory("seg_2", DirectoryOption::Mem());
    ASSERT_TRUE(nullptr != dynamic_cast<MemDirectory*>(seg2Dir->GetIDirectory().get()));
    auto indexWriter1 = seg2Dir->CreateFileWriter("indexfile1");
    ASSERT_EQ(FSEC_OK, indexWriter1->Write("32000", 5).Code());
    ASSERT_EQ(FSEC_OK, indexWriter1->Close());
    auto dataWriter1 = seg2Dir->CreateFileWriter("datafile1");
    ASSERT_EQ(FSEC_OK, dataWriter1->Write("32001", 5).Code());
    ASSERT_EQ(FSEC_OK, dataWriter1->Close());
    ValidateFileType(seg2Dir, "datafile1", "32001", FSFT_MEM);

    auto seg2DirOnRead = rtPartitionDir->GetDirectory("seg_2", false);
    ASSERT_TRUE(seg2DirOnRead);
    ASSERT_TRUE(nullptr != dynamic_cast<LocalDirectory*>(seg2DirOnRead->GetIDirectory().get()));
    ASSERT_TRUE(seg2DirOnRead->SetLifecycle("warm"));
    fs->Sync(true).GetOrThrow();
    ValidateFileType(seg2DirOnRead, "indexfile1", "32000", FSFT_MEM);
    ValidateFileType(seg2DirOnRead, "datafile1", "32001", FSFT_BLOCK);
}

void LogicalFileSystemIntetest::TestSegmentDynamicLifecycle()
{
    string loadConfigListJson = R"({
        "load_config": [
            {
                "file_patterns": [".*"],
                "load_strategy": "mmap",
                "deploy": true,
                "lifecycle": "hot"
            },
            {
                "file_patterns": [".*"],
                "load_strategy": "cache",
                "load_strategy_param": {
                   "cache_decompress_file": true,
                   "global_cache": true,
                   "direct_io": false
                },
                "lifecycle": "cold",
                "deploy": false,
                "remote": true
            }
        ]
    })";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, loadConfigListJson);
    auto logicalFs = CreateFSWithLoadConfig("test", false, loadConfigList, true);

    std::string testRoot = GetTestPath("");
    std::string localRoot = PathUtil::JoinPath(testRoot, "local_root/");
    std::string remoteRoot = PathUtil::JoinPath(testRoot, "remote_root/");

    Store("remote_root/seg_1/f1", "11000");
    Store("remote_root/seg_2/f1", "21000");
    // mimic deploy to local
    Store("local_root/seg_1/f1", "11000");

    logicalFs->SetDefaultRootPath(localRoot, remoteRoot);

    shared_ptr<IFileSystem> fs(logicalFs);
    auto ValidateFilePhysicalPathAndType = [&fs](const std::string& logicalPath, const std::string& expectPhysicalPath,
                                                 FSFileType type) {
        std::string physicalRoot;
        std::string relativePath;
        bool inPackage;
        bool isDir;
        ASSERT_TRUE(fs->TEST_GetPhysicalInfo(logicalPath, physicalRoot, relativePath, inPackage, isDir))
            << "LogicalPath:" << logicalPath << std::endl;
        EXPECT_EQ(expectPhysicalPath, PathUtil::JoinPath(physicalRoot, relativePath));
        auto [ec, reader] = fs->CreateFileReader(logicalPath, FSOT_LOAD_CONFIG);
        ASSERT_EQ(FSEC_OK, ec);
        auto fileNode = reader->GetFileNode();
        ASSERT_EQ(type, fileNode->GetType()) << "filePath: " << logicalPath << " type unexpected";
    };

    logicalFs->TEST_EnableSupportMmap(false);

    // test setting lifecycle will not change physicalRoot of a particular file
    ASSERT_TRUE(fs->SetDirLifecycle("seg_1", "hot"));
    ASSERT_TRUE(fs->SetDirLifecycle("seg_2", "cold"));

    ASSERT_EQ(FSEC_OK, fs->MountDir(localRoot, "seg_1/", "seg_1/", MountOption(FSMT_READ_ONLY), false).Code());
    ASSERT_EQ(FSEC_OK, fs->MountDir(remoteRoot, "seg_2/", "seg_2/", MountOption(FSMT_READ_ONLY), false).Code());

    ValidateFilePhysicalPathAndType("seg_1/f1", PathUtil::JoinPath(localRoot, "seg_1/f1"), FSFT_MEM);
    ValidateFilePhysicalPathAndType("seg_2/f1", PathUtil::JoinPath(remoteRoot, "seg_2/f1"), FSFT_BLOCK);

    ASSERT_TRUE(fs->SetDirLifecycle("seg_1", "cold"));
    ASSERT_TRUE(fs->SetDirLifecycle("seg_2", "hot"));
    ValidateFilePhysicalPathAndType("seg_1/f1", PathUtil::JoinPath(localRoot, "seg_1/f1"), FSFT_BLOCK);
    ValidateFilePhysicalPathAndType("seg_2/f1", PathUtil::JoinPath(remoteRoot, "seg_2/f1"), FSFT_MEM);

    // test MountDir will change physicalRoot of a particular file
    ASSERT_EQ(FSEC_OK, fs->MountDir(remoteRoot, "seg_1/", "seg_1/", MountOption(FSMT_READ_ONLY), false).Code());
    ValidateFilePhysicalPathAndType("seg_1/f1", PathUtil::JoinPath(remoteRoot, "seg_1/f1"), FSFT_BLOCK);
    ValidateFilePhysicalPathAndType("seg_2/f1", PathUtil::JoinPath(remoteRoot, "seg_2/f1"), FSFT_MEM);
}

void LogicalFileSystemIntetest::TestMountVersionWithDynamicLifecycle()
{
    string loadConfigListJson = R"({
        "load_config": [
            {
                "file_patterns": [".*"],
                "load_strategy": "mmap",
                "deploy": true,
                "lifecycle": "hot"
            },
            {
                "file_patterns": [".*"],
                "load_strategy": "cache",
                "load_strategy_param": {
                   "cache_decompress_file": true,
                   "global_cache": true,
                   "direct_io": false
                },
                "lifecycle": "cold",
                "deploy": false,
                "remote": true
            }
        ]
    })";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, loadConfigListJson);
    auto fs = CreateFSWithLoadConfig("test", false, loadConfigList, true);
    auto fsGuard = std::unique_ptr<file_system::LogicalFileSystem>(fs);
    std::string testRoot = GetTestPath("");
    std::string localRoot = PathUtil::JoinPath(testRoot, "local_root/");
    std::string remoteRoot = PathUtil::JoinPath(testRoot, "remote_root/");
    fs->SetDefaultRootPath(localRoot, remoteRoot);

    auto checkFileRoot = [&fs, localRoot, remoteRoot](const std::vector<std::string>& localFileList,
                                                      const std::vector<std::string>& remoteFileList) {
        for (const auto& file : localFileList) {
            auto [ec, entryMeta] = fs->GetEntryMeta(file);
            ASSERT_EQ(ec, FSEC_OK);
            std::string physicalPath = PathUtil::JoinPath(localRoot, file);
            ASSERT_EQ(entryMeta.GetFullPhysicalPath(), physicalPath);
        }
        for (const auto& file : remoteFileList) {
            auto [ec, entryMeta] = fs->GetEntryMeta(file);
            ASSERT_EQ(ec, FSEC_OK);
            std::string physicalPath = PathUtil::JoinPath(remoteRoot, file);
            ASSERT_EQ(entryMeta.GetFullPhysicalPath(), physicalPath);
        }
    };

    std::string entryTableJsonStr = R"(
    {
        "files": {
            "${LOCAL_ROOT}": {
                "s_0/f1": {"length": 10},
                "s_0/attr/f2": {"length": 10}
            },
            "${REMOTE_ROOT}": {
                "s_1/f1": {"length": 10},
                "s_1/attr/f2": {"length": 10}
            }
        }
    })";
    StringUtil::replaceAll(entryTableJsonStr, string("${REMOTE_ROOT}"), remoteRoot);
    StringUtil::replaceAll(entryTableJsonStr, string("${LOCAL_ROOT}"), localRoot);
    Store("entry_table.1", entryTableJsonStr);

    auto lifecycleTable = make_shared<LifecycleTable>();
    lifecycleTable->AddDirectory("s_0", "hot");
    lifecycleTable->AddDirectory("s_1", "cold");
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", FSMT_READ_ONLY, lifecycleTable));
    checkFileRoot({"s_0/f1", "s_0/attr/f2"}, {"s_1/f1", "s_1/attr/f2"});

    lifecycleTable.reset(new LifecycleTable);
    lifecycleTable->AddDirectory("s_0", "cold");
    lifecycleTable->AddDirectory("s_1", "hot");
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", FSMT_READ_ONLY, lifecycleTable));
    checkFileRoot({"s_1/f1", "s_1/attr/f2"}, {"s_0/f1", "s_0/attr/f2"});
}

void LogicalFileSystemIntetest::TestMountVersionWithCheckDiff()
{
    Store("entry_table.1", R"(
    {
        "files": {
            "dfs://storage/table/g_X/p_0": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "other_path/offset", "length": 19}
            },
            "dfs://storage/table/g_X/p_0/PATCH_ROOT.1": {
                "s_0/attr/age":   {"length": -2},
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age/offset":  {"length": 16}
            }
        }
    })");
    std::shared_ptr<LogicalFileSystem> fs = CreateFS();
    MountOption mountOption(FSMT_READ_ONLY);
    mountOption.conflictResolution = ConflictResolution::CHECK_DIFF;
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", mountOption, nullptr));
    CheckListFile(fs, "s_0", false, {"attr"});
    CheckListFile(fs, "s_0", true,
                  {"attr/", "attr/job/", "attr/name/", "attr/name/data", "attr/name/offset", "attr/age/",
                   "attr/age/data", "attr/age/offset"});

    CheckListFile(fs, "s_0/", false, {"attr"});
    CheckListFile(fs, "s_0/", true,
                  {"attr/", "attr/job/", "attr/name/", "attr/name/data", "attr/name/offset", "attr/age/",
                   "attr/age/data", "attr/age/offset"});

    CheckListFile(fs, "", false, {"entry_table.1", "s_0"});
    CheckListFile(fs, "", true,
                  {"s_0/", "s_0/attr/", "s_0/attr/job/", "s_0/attr/name/", "s_0/attr/name/data", "s_0/attr/name/offset",
                   "s_0/attr/age/", "s_0/attr/age/data", "s_0/attr/age/offset"});
    ASSERT_EQ(FSEC_OK, fs->MountVersion(GetTestPath(), 1, "", mountOption, nullptr));

    FileList fileList;
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s", ListOption::Recursive(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s", ListOption(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s_0x", ListOption::Recursive(), fileList));
    ASSERT_EQ(FSEC_NOENT, fs->ListDir("s_0x", ListOption(), fileList));

    Store("entry_table.2", R"(
    {
        "files": {
            "dfs://storage/table/g_X2/p_0": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "other_path/offset", "length": 19}
            },
            "dfs://storage/table/g_X2/p_0/PATCH_ROOT.1": {
                "s_0/attr/age":   {"length": -2},
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age/offset":  {"length": 16}
            }
        }
    })");
    ASSERT_EQ(FSEC_BADARGS, fs->MountVersion(GetTestPath(), 2, "", mountOption, nullptr));
}

}} // namespace indexlib::file_system
