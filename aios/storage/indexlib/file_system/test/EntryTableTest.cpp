#include "indexlib/file_system/EntryTable.h"

#include "gtest/gtest.h"
#include <algorithm>
#include <memory>
#include <ostream>
#include <stdint.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace file_system {

class EntryTableTest : public INDEXLIB_TESTBASE
{
public:
    EntryTableTest();
    ~EntryTableTest();

    DECLARE_CLASS_NAME(EntryTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestClass();
    void TestJsonize();
    void TestListDir();
    void TestFreeze();
    void TestValidatePackageFileLengths();
    void TestCompareFile();
    void TestUpdateAfterCommit();
    void TestCalculateFileSize();
    void TestCalculateFileSize4Abnormal();

private:
    void CheckListFile(std::shared_ptr<EntryTable> entryTable, const std::string& dir, bool isRecursive,
                       std::multiset<std::string> expectList);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, EntryTableTest);

INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestClass);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestListDir);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestFreeze);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestValidatePackageFileLengths);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestCompareFile);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestUpdateAfterCommit);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestCalculateFileSize);
INDEXLIB_UNIT_TEST_CASE(EntryTableTest, TestCalculateFileSize4Abnormal);

//////////////////////////////////////////////////////////////////////

EntryTableTest::EntryTableTest() {}

EntryTableTest::~EntryTableTest() {}

void EntryTableTest::CaseSetUp() {}

void EntryTableTest::CaseTearDown() {}

void EntryTableTest::TestClass()
{
    // sizeof
    EXPECT_EQ(sizeof(autil::legacy::Jsonizable) + sizeof(string) + sizeof(std::string*) * 3 + sizeof(int64_t) * 2,
              sizeof(EntryMeta));

    // default value
    EntryMeta em;
    EXPECT_TRUE(em.IsFile());
    EXPECT_FALSE(em.IsDir());
    EXPECT_FALSE(em.IsNoEnt());
    EXPECT_FALSE(em.IsInPackage());
    EXPECT_FALSE(em.IsOwner());
    EXPECT_FALSE(em.IsMutable());
    EXPECT_EQ(0, em.GetLength());

    // bit field
    em.SetIsMemFile(true);
    em.SetNoEnt();
    em.SetMutable(false);
    em.SetOwner(true);
    em._reserved = (unsigned char)0x7f;
    em.SetOffset(7);
    EXPECT_EQ(0x6ff0000000000007, em._unionAttribute)
        << em.IsMemFile() << em.IsOwner() << "," << em.IsMutable() << "," << em.GetOffset();
}

void EntryTableTest::TestJsonize()
{
    string jsonStr = R"(
    {
        "files": {
            "": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "/other_path/offset", "length": 19},
                "s_1":   {"length": -2},
                "s_1/attr":   {"length": -2},
                "s_1/attr/name":   {"length": -2}
            },
            "dfs://storage/table/g_X/p_0/PATCH_ROOT.1/": {
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age":    {"length": -2},
                "s_0/attr/age/offset":  {"length": 16}
            }
        },
        "package_files": {
            "": {
                "s_1/package_file.__meta__": { "length": 89 },
                "s_1/package_file.__data__.MERGE.0": {
                    "length": 4104,
                    "files": {
                        "s_1/attr/name/data":   { "offset": 0,    "length": 94 },
                        "s_1/attr/name/offset": { "offset": 4096, "length": 8 }
                    }
                },
                "s_1/package_file.__data__.PATCH.1": {
                    "length": 4107,
                    "files": {
                    }
                },
                "s_2/package_file.__meta__": { "length": 16 },
                "s_2/package_file.__data__.MERGER.0": {
                }
            }
        }
    })";

    std::shared_ptr<FileSystemOptions> fsOptions(new FileSystemOptions());
    EntryTableBuilder entryTableBuilder;
    std::shared_ptr<EntryTable> entryTable = entryTableBuilder.CreateEntryTable(
        /*name=*/"test", /*outputRoot=*/"dfs://storage/table/g_X/p_0", fsOptions);
    EXPECT_EQ(FSEC_OK, entryTableBuilder.TEST_FromEntryTableString(jsonStr, "dfs://storage/table/g_X/p_0"));

    CheckListFile(entryTable, "s_0", /*isRecursive=*/false, {"attr"});
    CheckListFile(entryTable, "s_0", /*isRecursive=*/true,
                  {"attr", "attr/job", "attr/name", "attr/name/data", "attr/name/offset", "attr/age", "attr/age/data",
                   "attr/age/offset"});

    CheckListFile(entryTable, "", /*isRecursive=*/false, {"s_0", "s_1"});
    CheckListFile(entryTable, "", /*isRecursive=*/true,
                  {"s_0", "s_0/attr", "s_0/attr/job", "s_0/attr/name", "s_0/attr/name/data", "s_0/attr/name/offset",
                   "s_0/attr/age", "s_0/attr/age/data", "s_0/attr/age/offset", "s_1", "s_1/attr", "s_1/attr/name",
                   "s_1/attr/name/data", "s_1/attr/name/offset"});

    CheckListFile(entryTable, "s", /*isRecursive=*/true, {});
    CheckListFile(entryTable, "s", /*isRecursive=*/false, {});
    CheckListFile(entryTable, "s_0x", /*isRecursive=*/true, {});
    CheckListFile(entryTable, "s_0x", /*isRecursive=*/false, {});

    string physicalPath;
    EXPECT_TRUE(entryTable->TEST_GetPhysicalPath("s_0/attr/name/data", physicalPath));
    EXPECT_EQ(physicalPath, "dfs://storage/table/g_X/p_0/s_0/attr/name/data");
    EXPECT_TRUE(entryTable->TEST_GetPhysicalPath("s_0/attr/age/data", physicalPath));
    EXPECT_EQ(physicalPath, "dfs://storage/table/g_X/p_0/PATCH_ROOT.1/s_0/attr/age/data");
    // Dir does not have physical path.
    EXPECT_TRUE(entryTable->TEST_GetPhysicalPath("s_0/attr", physicalPath));
    EXPECT_FALSE(autil::StringUtil::startsWith(physicalPath, "null/"));
    // Test package files behavior
    EXPECT_TRUE(entryTable->TEST_GetPhysicalPath("s_1/attr/name/data", physicalPath));
    EXPECT_EQ("dfs://storage/table/g_X/p_0/s_1/package_file.__data__.MERGE.0", physicalPath);
    EXPECT_TRUE(entryTable->TEST_GetPhysicalPath("s_1/attr", physicalPath));
    EXPECT_FALSE(autil::StringUtil::startsWith(physicalPath, "null/"));

    string originalJsonStr;
    ASSERT_EQ(entryTable->ToString({}, {}, {}, &originalJsonStr), FSEC_OK);

    std::shared_ptr<EntryTable> newEntryTable = entryTableBuilder.CreateEntryTable(
        /*name=*/"test", /*outputRoot=*/"dfs://storage/table/g_X/p_0", fsOptions);
    ASSERT_EQ(FSEC_OK, entryTableBuilder.TEST_FromEntryTableString(originalJsonStr, "dfs://storage/table/g_X/p_0"));
    string rebuiltJsonStr;
    ASSERT_EQ(newEntryTable->ToString({}, {}, {}, &rebuiltJsonStr), FSEC_OK);
    EXPECT_EQ(originalJsonStr, rebuiltJsonStr);

    newEntryTable = entryTableBuilder.CreateEntryTable(
        /*name=*/"test", /*outputRoot=*/"/OtherRoot", fsOptions);
    ASSERT_EQ(FSEC_OK,
              entryTableBuilder.TEST_FromEntryTableString(originalJsonStr, "dfs://other-storage/table/g_X/p_0"));

    EXPECT_TRUE(newEntryTable->TEST_GetPhysicalPath("s_0/attr/name/data", physicalPath));
    EXPECT_EQ(physicalPath, "dfs://other-storage/table/g_X/p_0/s_0/attr/name/data");
    EXPECT_TRUE(newEntryTable->TEST_GetPhysicalPath("s_0/attr/age/data", physicalPath));
    EXPECT_EQ(physicalPath, "dfs://storage/table/g_X/p_0/PATCH_ROOT.1/s_0/attr/age/data");
    EXPECT_TRUE(newEntryTable->TEST_GetPhysicalPath("s_1/attr/name/data", physicalPath));
    EXPECT_EQ("dfs://other-storage/table/g_X/p_0/s_1/package_file.__data__.MERGE.0", physicalPath);
}

void EntryTableTest::CheckListFile(std::shared_ptr<EntryTable> entryTable, const string& dir, bool isRecursive,
                                   multiset<string> expectList)
{
    int startIndex = 0;
    if (!dir.empty()) {
        startIndex = dir.size() + 1;
    }

    fslib::FileList fileList;
    auto metas = entryTable->ListDir(dir, isRecursive);
    for (auto& meta : metas) {
        fileList.emplace_back(PathUtil::NormalizePath(meta.GetLogicalPath().substr(startIndex)));
    }
    EXPECT_EQ(expectList, multiset<string>(fileList.begin(), fileList.end()));
}

void EntryTableTest::TestListDir()
{
    std::shared_ptr<EntryTable> entryTable(new EntryTable("", "", false));
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/a", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir-xx", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir-xx/b", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dira", "", nullptr)).ec);

    CheckListFile(entryTable, "", true, {"dir", "dir/a", "dir-xx", "dir-xx/b", "dira"});
    CheckListFile(entryTable, "", false, {"dir", "dir-xx", "dira"});
    CheckListFile(entryTable, "d", true, {});
    CheckListFile(entryTable, "d", false, {});
    CheckListFile(entryTable, "dir", true, {"a"});
    CheckListFile(entryTable, "dir", false, {"a"});
    CheckListFile(entryTable, "dir-xx", true, {"b"});
    CheckListFile(entryTable, "dir-xx", false, {"b"});
}

void EntryTableTest::TestFreeze()
{
    std::shared_ptr<EntryTable> entryTable(new EntryTable("", "", false));
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("a", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->Freeze("a"));
    ASSERT_EQ(FSEC_NOENT, entryTable->Freeze("b"));
}

void EntryTableTest::TestValidatePackageFileLengths()
{
    std::shared_ptr<EntryTable> entryTable(new EntryTable("", "", false));
    entryTable->SetPackageFileLength("dfs://seg_0/package_file.__data__", 23);
    entryTable->SetPackageFileLength("dfs://seg_0/package_file.__meta__", 12);
    ASSERT_TRUE(entryTable->ValidatePackageFileLengths());
    entryTable->SetPackageFileLength("dfs://seg_1/package_file.__meta__", 12);
    ASSERT_TRUE(entryTable->ValidatePackageFileLengths());
    entryTable->SetPackageFileLength("dfs://seg_2/package_file.__data__", 12);
    ASSERT_FALSE(entryTable->ValidatePackageFileLengths());
}

void EntryTableTest::TestCompareFile()
{
    EXPECT_TRUE(EntryTableBuilder::CompareFile("/root/a", "/root/a/b"));
    EXPECT_FALSE(EntryTableBuilder::CompareFile("/root/a/b", "/root/a"));

    EXPECT_TRUE(EntryTableBuilder::CompareFile("/root/a", "/root/b"));
    EXPECT_FALSE(EntryTableBuilder::CompareFile("/root/b", "/root/a"));

    EXPECT_TRUE(EntryTableBuilder::CompareFile("/root/a/xyz", "/root/a/package_file.__meta__.abc.1"));
    EXPECT_FALSE(EntryTableBuilder::CompareFile("/root/a/package_file.__meta__.abc.1", "/root/a/xyz"));

    EXPECT_TRUE(EntryTableBuilder::CompareFile("/root/a", "/root/a/package_file.__meta__.abc.1"));
    EXPECT_FALSE(EntryTableBuilder::CompareFile("/root/a/package_file.__meta__.abc.1", "/root/a"));

    EXPECT_TRUE(
        EntryTableBuilder::CompareFile("/root/a/package_file.__meta__.abc.2", "/root/b/package_file.__meta__.abc.1"));
    EXPECT_FALSE(
        EntryTableBuilder::CompareFile("/root/b/package_file.__meta__.abc.1", "/root/a/package_file.__meta__.abc.2"));

    EXPECT_TRUE(EntryTableBuilder::CompareFile("/root/a/package_file.__meta__", "/root/b/package_file.__meta__"));
    EXPECT_FALSE(EntryTableBuilder::CompareFile("/root/b/package_file.__meta__", "/root/a/package_file.__meta__"));

    EXPECT_TRUE(
        EntryTableBuilder::CompareFile("/root/package_file.__meta__.abc.2", "/root/b/package_file.__meta__.efg.1"));
    EXPECT_FALSE(
        EntryTableBuilder::CompareFile("/root/b/package_file.__meta__.efg.1", "/root/package_file.__meta__.abc.2"));

    EXPECT_TRUE(
        EntryTableBuilder::CompareFile("/root/package_file.__meta__.abc.1", "/root/package_file.__meta__.abc.2"));
    EXPECT_FALSE(
        EntryTableBuilder::CompareFile("/root/package_file.__meta__.abc.2", "/root/package_file.__meta__.abc.1"));

    EXPECT_TRUE(
        EntryTableBuilder::CompareFile("/root/package_file.__meta__.abc.2", "/root/package_file.__meta__.abc.11"));
    EXPECT_FALSE(
        EntryTableBuilder::CompareFile("/root/package_file.__meta__.abc.11", "/root/package_file.__meta__.abc.2"));
}

void EntryTableTest::TestUpdateAfterCommit()
{
    std::shared_ptr<EntryTable> entryTable(new EntryTable("", "", false));
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/file1", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/file2", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir1", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/subdir1/file", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir1/subdir", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/subdir1/subdir/file", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir2", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/subdir2/file", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir3", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta("dir/subdir3/file", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir4", "", nullptr)).ec);
    ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(EntryMeta::Dir("dir/subdir5", "", nullptr)).ec);

    std::vector<EntryMeta> metas = entryTable->ListDir(/*dir=*/"", /*recursive=*/true);
    for (EntryMeta& meta : metas) {
        meta.SetMutable(true);
        meta.SetOwner(true);
        ASSERT_EQ(FSEC_OK, entryTable->AddEntryMeta(meta).ec);
    }

    entryTable->UpdateAfterCommit(/*wishFileList=*/ {"dir/file1", "dir/subdir2/file", "non-exist-file"},
                                  /*wishDirList=*/ {"dir/subdir1", "dir/subdir5", "dir/non-exist-dir"});

    EXPECT_TRUE(entryTable->GetEntryMeta("dir").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/file1").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/file1").result.IsOwner());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/file2").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/file2").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1/subdir").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1/subdir").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1/subdir/file").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir1/subdir/file").result.IsOwner());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir2").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir2").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir2/file").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir2/file").result.IsOwner());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir3").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir3").result.IsOwner());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir3/file").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir3/file").result.IsOwner());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir4").result.IsMutable());
    EXPECT_TRUE(entryTable->GetEntryMeta("dir/subdir4").result.IsOwner());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir5").result.IsMutable());
    EXPECT_FALSE(entryTable->GetEntryMeta("dir/subdir5").result.IsOwner());
}

void EntryTableTest::TestCalculateFileSize()
{
    string jsonStr = R"(
    {
        "files": {
            "": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "/other_path/offset", "length": 19},
                "s_1":   {"length": -2},
                "s_1/attr":   {"length": -2},
                "s_1/attr/name":   {"length": -2}
            },
            "dfs://storage/table/g_X/p_0/PATCH_ROOT.1/": {
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age":    {"length": -2},
                "s_0/attr/age/offset":  {"length": 16}
            }
        },
        "package_files": {
            "": {
                "s_1/package_file.__meta__": { "length": 89 },
                "s_1/package_file.__data__.MERGE.0": {
                    "length": 4104,
                    "files": {
                        "s_1/attr/name/data":   { "offset": 0,    "length": 94 },
                        "s_1/attr/name/offset": { "offset": 4096, "length": 8 }
                    }
                },
                "s_1/package_file.__data__.PATCH.1": {
                    "length": 4107,
                    "files": {
                    }
                },
                "s_2/package_file.__meta__": { "length": 16 },
                "s_2/package_file.__data__.MERGER.0": {
                }
            }
        }
    })";

    std::shared_ptr<FileSystemOptions> fsOptions(new FileSystemOptions());
    EntryTableBuilder entryTableBuilder;
    std::shared_ptr<EntryTable> entryTable = entryTableBuilder.CreateEntryTable(
        /*name=*/"test", /*outputRoot=*/"dfs://storage/table/g_X/p_0", fsOptions);
    EXPECT_EQ(FSEC_OK, entryTableBuilder.TEST_FromEntryTableString(jsonStr, "dfs://storage/table/g_X/p_0"));

    auto fileSize = entryTable->CalculateFileSize();
    ASSERT_EQ(8397, fileSize);
}

void EntryTableTest::TestCalculateFileSize4Abnormal()
{
    // 正常情况下, package_file** 不应该出现在files下
    // 本case主要是验证出现的情况下, 不会重复统计
    string jsonStr = R"(
    {
        "files": {
            "": {
                "s_0":   {"length": -2},
                "s_0/attr":   {"length": -2},
                "s_0/attr/job":   {"length": -2},
                "s_0/attr/name":   {"length": -2},
                "s_0/attr/name/data":   {"length": 23},
                "s_0/attr/name/offset": {"path": "/other_path/offset", "length": 19},
                "s_1":   {"length": -2},
                "s_1/attr":   {"length": -2},
                "s_1/attr/name":   {"length": -2},
                "s_1/package_file.__meta__": { "length": 89 },
                "s_1/package_file.__data__.MERGE.0": { "length": 4104 },
                "s_1/attr/name/data": { "length": 94 }
            },
            "dfs://storage/table/g_X/p_0/PATCH_ROOT.1/": {
                "s_0/attr/age/data":    {"length": 23},
                "s_0/attr/age":    {"length": -2},
                "s_0/attr/age/offset":  {"length": 16}
            }
        },
        "package_files": {
            "": {
                "s_1/package_file.__meta__": { "length": 89 },
                "s_1/package_file.__data__.MERGE.0": {
                    "length": 4104,
                    "files": {
                        "s_1/attr/name/data":   { "offset": 0,    "length": 94 },
                        "s_1/attr/name/offset": { "offset": 4096, "length": 8 }
                    }
                },
                "s_1/package_file.__data__.PATCH.1": {
                    "length": 4107,
                    "files": {
                    }
                },
                "s_2/package_file.__meta__": { "length": 16 },
                "s_2/package_file.__data__.MERGER.0": {
                }
            }
        }
    })";

    std::shared_ptr<FileSystemOptions> fsOptions(new FileSystemOptions());
    EntryTableBuilder entryTableBuilder;
    std::shared_ptr<EntryTable> entryTable = entryTableBuilder.CreateEntryTable(
        /*name=*/"test", /*outputRoot=*/"dfs://storage/table/g_X/p_0", fsOptions);
    EXPECT_EQ(FSEC_OK, entryTableBuilder.TEST_FromEntryTableString(jsonStr, "dfs://storage/table/g_X/p_0"));

    auto fileSize = entryTable->CalculateFileSize();
    ASSERT_EQ(8397, fileSize);
}

}} // namespace indexlib::file_system
