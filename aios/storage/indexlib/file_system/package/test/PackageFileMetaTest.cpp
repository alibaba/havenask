

#include "indexlib/file_system/package/PackageFileMeta.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/test/PackageTestUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace file_system {
class PackageOpenMeta;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class PackageFileMetaTest : public INDEXLIB_TESTBASE
{
public:
    PackageFileMetaTest();
    ~PackageFileMetaTest();

    DECLARE_CLASS_NAME(PackageFileMetaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitFromFileNode();
    void TestInitFromString();
    void TestToString();
    void TestGetRelativeFilePath();
    void TestGetPackageDataFileLength();
    void TestGetPackageDataFileIdx();
    void TestIsPackageFileName();

private:
    void InnerTestInitFromFileNodes(const std::string& fileNodesDespStr, size_t fileAlignSize);
    std::vector<std::shared_ptr<FileNode>> InitFileNodes(const std::string& fileNodesDespStr,
                                                         const std::string& packageFilePath);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileMetaTest);

INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestInitFromFileNode);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestInitFromString);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestToString);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetRelativeFilePath);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetPackageDataFileLength);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetPackageDataFileIdx);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestIsPackageFileName);
//////////////////////////////////////////////////////////////////////

namespace {
class FakeFileNode : public FileNode
{
public:
    FakeFileNode(const std::string path, size_t length, FSFileType type) : _length(length), _type(type)
    {
        // _logicalPath = path;
        _physicalPath = path;
    }
    ~FakeFileNode() {}

public:
    FSFileType GetType() const noexcept override { return _type; }
    size_t GetLength() const noexcept override { return _length; }

    void* GetBaseAddress() const noexcept override { return NULL; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override
    {
        assert(false);
        return {FSEC_OK, 0};
    }

    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override
    {
        assert(false);
        return NULL;
    }

    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override
    {
        assert(false);
        return {FSEC_OK, 0};
    }
    FSResult<void> Close() noexcept override { return FSEC_OK; }
    bool ReadOnly() const noexcept override { return false; }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }

private:
    size_t _length;
    FSFileType _type;
};

}; // namespace

PackageFileMetaTest::PackageFileMetaTest() {}

PackageFileMetaTest::~PackageFileMetaTest() {}

void PackageFileMetaTest::CaseSetUp() {}

void PackageFileMetaTest::CaseTearDown() {}

void PackageFileMetaTest::TestInitFromFileNode()
{
    InnerTestInitFromFileNodes("index:DIR:0:0:0#index/file1:FILE:5:0:0#"
                               "file2:FILE:100:64:0#attribute:DIR:0:0:0",
                               64);
    InnerTestInitFromFileNodes("", 64);
    InnerTestInitFromFileNodes("index:DIR:0:0:0", 64);
    InnerTestInitFromFileNodes("file1:FILE:65:0:0#file2:FILE:5:128:0", 64);
}

void PackageFileMetaTest::TestInitFromString()
{
    string jsonString = "{                      \
    \"inner_files\": [                          \
        {                                       \
            \"path\" : \"index\",               \
            \"offset\" : 0,                     \
            \"length\" : 0,                     \
            \"fileIdx\" : 0,                    \
            \"isDir\" : true                    \
        },                                      \
        {                                       \
            \"path\" : \"file1\",               \
            \"offset\" : 23,                    \
            \"length\" : 123,                   \
            \"fileIdx\" : 3,                    \
            \"isDir\" : false                   \
        }                                       \
    ],                                          \
    \"file_align_size\" : 64                    \
}";

    PackageFileMeta packageFileMeta;
    ASSERT_EQ(FSEC_OK, packageFileMeta.InitFromString(jsonString));

    PackageTestUtil::CheckInnerFileMeta("index:DIR:0:0:0#file1:FILE:123:23:3", packageFileMeta);
    ASSERT_EQ((size_t)64, packageFileMeta.GetFileAlignSize());
    ASSERT_EQ((size_t)146, packageFileMeta.GetPhysicalFileLength(3));
    ASSERT_EQ((size_t)0, packageFileMeta.GetPhysicalFileLength(0));
}

void PackageFileMetaTest::TestGetPackageDataFileLength()
{
    {
        string jsonString = "{                      \
    \"inner_files\": [                              \
        {                                           \
            \"path\" : \"index\",                   \
            \"offset\" : 0,                         \
            \"length\" : 0,                         \
            \"fileIdx\" : 1,                        \
            \"isDir\" : true                        \
        },                                          \
        {                                           \
            \"path\" : \"file1\",                   \
            \"offset\" : 23,                        \
            \"length\" : 123,                       \
            \"fileIdx\" : 3,                        \
            \"isDir\" : false                       \
        },                                          \
        {                                           \
            \"path\" : \"file4\",                   \
            \"offset\" : 22,                        \
            \"length\" : 100,                       \
            \"fileIdx\" : 1,                        \
            \"isDir\" : false                       \
        },                                          \
        {                                           \
            \"path\" : \"file2\",                   \
            \"offset\" : 0,                         \
            \"length\" : 0,                         \
            \"fileIdx\" : 1,                        \
            \"isDir\" : true                        \
        }                                           \
    ],                                              \
    \"file_align_size\" : 64                        \
}";

        PackageFileMeta packageFileMeta;
        ASSERT_EQ(FSEC_OK, packageFileMeta.InitFromString(jsonString));

        PackageTestUtil::CheckInnerFileMeta("index:DIR:0:0:1#file1:FILE:123:23:3#file4:FILE:100:22:1#file2:DIR:0:0:1",
                                            packageFileMeta);
        ASSERT_EQ((size_t)64, packageFileMeta.GetFileAlignSize());
        ASSERT_EQ((size_t)146, packageFileMeta.GetPhysicalFileLength(3));
        ASSERT_EQ((size_t)122, packageFileMeta.GetPhysicalFileLength(1));
    }
    {
        string jsonString = "{                      \
    \"inner_files\": [                              \
        {                                           \
            \"path\" : \"index\",                   \
            \"offset\" : 0,                         \
            \"length\" : 0,                         \
            \"fileIdx\" : 1,                        \
            \"isDir\" : true                        \
        },                                          \
        {                                           \
            \"path\" : \"file1\",                   \
            \"offset\" : 23,                        \
            \"length\" : 123,                       \
            \"fileIdx\" : 3,                        \
            \"isDir\" : false                       \
        },                                          \
        {                                           \
            \"path\" : \"file2\",                   \
            \"offset\" : 0,                         \
            \"length\" : 0,                         \
            \"fileIdx\" : 1,                        \
            \"isDir\" : true                        \
        }                                           \
    ],                                              \
    \"file_align_size\" : 64                        \
}";

        PackageFileMeta packageFileMeta;
        ASSERT_EQ(FSEC_OK, packageFileMeta.InitFromString(jsonString));

        PackageTestUtil::CheckInnerFileMeta("index:DIR:0:0:1#file1:FILE:123:23:3#file2:DIR:0:0:1", packageFileMeta);
        ASSERT_EQ((size_t)64, packageFileMeta.GetFileAlignSize());
        ASSERT_EQ((size_t)0, packageFileMeta.GetPhysicalFileLength(1));
    }
}

void PackageFileMetaTest::TestToString()
{
    string despStr = "index:DIR:0:0:0#index/file1:FILE:5:0:0#"
                     "file2:FILE:100:64:0#attribute:DIR:0:0:0";
    size_t fileAlignSize = 64;

    string packageFilePath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "package_file");
    vector<std::shared_ptr<FileNode>> fileNodes = InitFileNodes(despStr, packageFilePath);

    PackageFileMeta packageFileMeta;
    ASSERT_EQ(FSEC_OK, packageFileMeta.InitFromFileNode(PathUtil::GetParentDirPath(packageFilePath), packageFilePath,
                                                        fileNodes, fileAlignSize));

    string jsonStr;
    ASSERT_EQ(packageFileMeta.ToString(&jsonStr), FSEC_OK);
    PackageFileMeta fromStrFileMeta;
    ASSERT_EQ(FSEC_OK, fromStrFileMeta.InitFromString(jsonStr));

    PackageTestUtil::CheckInnerFileMeta(despStr, fromStrFileMeta);
    ASSERT_EQ(fileAlignSize, fromStrFileMeta.GetFileAlignSize());
}

void PackageFileMetaTest::TestGetRelativeFilePath()
{
    PackageFileMeta packageFileMeta;
    string relpath;
    ASSERT_EQ(packageFileMeta.GetRelativeFilePath("abc/def", "abc/def/package", &relpath), FSEC_OK);
    ASSERT_EQ(string("package"), relpath);

    ASSERT_EQ(packageFileMeta.GetRelativeFilePath("abc/", "abc/def/package", &relpath), FSEC_OK);
    ASSERT_EQ(string("def/package"), relpath);

    ASSERT_EQ(packageFileMeta.GetRelativeFilePath("abc/def/package/", "abc/def/package/", &relpath), FSEC_OK);
    ASSERT_EQ(string(""), relpath);

    ASSERT_EQ(packageFileMeta.GetRelativeFilePath("abc/def/package", "abc/def/package", &relpath), FSEC_OK);
    ASSERT_EQ(string(""), relpath);

    ASSERT_EQ(packageFileMeta.GetRelativeFilePath("abc/def", "abc/non_exist/package", &relpath), FSEC_ERROR);
}

vector<std::shared_ptr<FileNode>> PackageFileMetaTest::InitFileNodes(const string& fileNodesDespStr,
                                                                     const string& packageFilePath)
{
    vector<std::shared_ptr<FileNode>> fileNodes;
    vector<vector<string>> nodeInfos;
    // path:opentype:length:offset:dataIdx
    StringUtil::fromString(fileNodesDespStr, nodeInfos, ":", "#");

    for (size_t i = 0; i < nodeInfos.size(); ++i) {
        string dirPath = PathUtil::GetParentDirPath(packageFilePath);
        string absPath = PathUtil::JoinPath(dirPath, nodeInfos[i][0]);
        FSFileType type = nodeInfos[i][1] == "DIR" ? FSFT_DIRECTORY : FSFT_MEM;
        size_t length = StringUtil::numberFromString<size_t>(nodeInfos[i][2]);
        fileNodes.push_back(std::shared_ptr<FileNode>(new FakeFileNode(absPath, length, type)));
    }
    return fileNodes;
}

// path:opentype:length:offset:dataIdx#path:opentype:length:offset:dataIdx
void PackageFileMetaTest::InnerTestInitFromFileNodes(const string& fileNodesDespStr, size_t fileAlignSize)
{
    string packageFilePath = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "package_file");
    vector<std::shared_ptr<FileNode>> fileNodes = InitFileNodes(fileNodesDespStr, packageFilePath);
    PackageFileMeta packageFileMeta;
    ASSERT_EQ(FSEC_OK, packageFileMeta.InitFromFileNode(PathUtil::GetParentDirPath(packageFilePath), packageFilePath,
                                                        fileNodes, fileAlignSize));
    PackageTestUtil::CheckInnerFileMeta(fileNodesDespStr, packageFileMeta);
}

void PackageFileMetaTest::TestGetPackageDataFileIdx()
{
    ASSERT_EQ(0, PackageFileMeta::GetPackageDataFileIdx("package_file.__data__0"));
    ASSERT_EQ(-1, PackageFileMeta::GetPackageDataFileIdx("package_file.__data__x"));
    ASSERT_EQ(-1, PackageFileMeta::GetPackageDataFileIdx("package_file__data__0"));
    ASSERT_EQ(100, PackageFileMeta::GetPackageDataFileIdx("file.__data__100"));
}

void PackageFileMetaTest::TestIsPackageFileName()
{
    ASSERT_EQ(make_pair(true, true), PackageFileMeta::IsPackageFileName("package_file.__meta__"));
    ASSERT_EQ(make_pair(true, false), PackageFileMeta::IsPackageFileName("package_file.__data__"));
    ASSERT_EQ(make_pair(true, false), PackageFileMeta::IsPackageFileName("package_file.__data__.MERGE.1"));

    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName("package_file__data__"));
    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName("package_fil.__meta__"));
    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName(""));
    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName("p"));
    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName("package_file..__meta__"));
    ASSERT_EQ(make_pair(false, false), PackageFileMeta::IsPackageFileName("package_file..__data__"));
}

TEST_F(PackageFileMetaTest, GetDataFileIdx)
{
    EXPECT_EQ(0, PackageFileMeta::GetDataFileIdx("package_file.__data__0"));
    EXPECT_EQ(123, PackageFileMeta::GetDataFileIdx("package_file.__data__123"));
    EXPECT_EQ(0, PackageFileMeta::GetDataFileIdx("package_file.__data__.random_tag.0"));
    EXPECT_EQ(456, PackageFileMeta::GetDataFileIdx("package_file.__data__.random_tag.456"));
    EXPECT_EQ(-1, PackageFileMeta::GetDataFileIdx("package_file.__data__"));
    EXPECT_EQ(-1, PackageFileMeta::GetDataFileIdx("package_file.__data__.random_tag.1.3"));
    EXPECT_EQ(-1, PackageFileMeta::GetDataFileIdx("package_file.__data__.1"));
}

}} // namespace indexlib::file_system
