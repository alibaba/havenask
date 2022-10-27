#include "indexlib/file_system/test/package_file_meta_unittest.h"
#include "indexlib/file_system/file_system_define.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileMetaTest);


namespace 
{
class FakeFileNode : public FileNode 
{
public:
    FakeFileNode(const std::string path,
                 size_t length, FSFileType type)
        : mLength(length)
        , mType(type)
    {
        mPath = path;
    }
    ~FakeFileNode() {}

public:

    FSFileType GetType() const { return mType; } 
    size_t GetLength() const { return mLength; }
    
    void* GetBaseAddress() const { return NULL; }
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option) 
    {
        assert(false);
        return 0;
    }

    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option)
    {
        assert(false);
        return NULL;
    }

    size_t Write(const void* buffer, size_t length)
    {
        assert(false);
        return 0;
    }
    void Close() {}
    bool ReadOnly() const override { return false;  }

private:
    void DoOpen(const std::string& path, FSOpenType openType) 
    { assert(false);}
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
    { assert(false);}
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType)
    { assert(false);}

private:
    size_t mLength;
    FSFileType mType;
};


};


PackageFileMetaTest::PackageFileMetaTest()
{
}

PackageFileMetaTest::~PackageFileMetaTest()
{
}

void PackageFileMetaTest::CaseSetUp()
{
}

void PackageFileMetaTest::CaseTearDown()
{
}

void PackageFileMetaTest::TestInitFromFileNode()
{
    InnerTestInitFromFileNodes("index:DIR:0:0:0#index/file1:FILE:5:0:0#"
                               "file2:FILE:100:64:0#attribute:DIR:0:0:0", 64);
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
    packageFileMeta.InitFromString(jsonString);

    CheckInnerFileMeta("index:DIR:0:0:0#file1:FILE:123:23:3", packageFileMeta);
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
        packageFileMeta.InitFromString(jsonString);

        CheckInnerFileMeta("index:DIR:0:0:1#file1:FILE:123:23:3#file4:FILE:100:22:1#file2:DIR:0:0:1", packageFileMeta);
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
        packageFileMeta.InitFromString(jsonString);

        CheckInnerFileMeta("index:DIR:0:0:1#file1:FILE:123:23:3#file2:DIR:0:0:1", packageFileMeta);
        ASSERT_EQ((size_t)64, packageFileMeta.GetFileAlignSize());
        ASSERT_EQ((size_t)0, packageFileMeta.GetPhysicalFileLength(1));
    }
}

void PackageFileMetaTest::TestToString()
{
    string despStr = "index:DIR:0:0:0#index/file1:FILE:5:0:0#"
                     "file2:FILE:100:64:0#attribute:DIR:0:0:0";
    size_t fileAlignSize = 64;

    string packageFilePath = PathUtil::JoinPath(
            GET_TEST_DATA_PATH(), "package_file");
    vector<FileNodePtr> fileNodes = InitFileNodes(
            despStr, packageFilePath);

    PackageFileMeta packageFileMeta;
    packageFileMeta.InitFromFileNode(packageFilePath, 
            fileNodes, fileAlignSize);

    string jsonStr = packageFileMeta.ToString();
    PackageFileMeta fromStrFileMeta;
    fromStrFileMeta.InitFromString(jsonStr);

    CheckInnerFileMeta(despStr, fromStrFileMeta);
    ASSERT_EQ(fileAlignSize, fromStrFileMeta.GetFileAlignSize());
}

void PackageFileMetaTest::TestGetRelativeFilePath()
{
    PackageFileMeta packageFileMeta;
    string relpath = packageFileMeta.GetRelativeFilePath(
            "abc/def", "abc/def/package");
    ASSERT_EQ(string("package"), relpath);

    relpath = packageFileMeta.GetRelativeFilePath(
            "abc/", "abc/def/package");
    ASSERT_EQ(string("def/package"), relpath);

    relpath = packageFileMeta.GetRelativeFilePath(
            "abc/def/package/", "abc/def/package/");
    ASSERT_EQ(string(""), relpath);

    relpath = packageFileMeta.GetRelativeFilePath(
            "abc/def/package", "abc/def/package");
    ASSERT_EQ(string(""), relpath);


    ASSERT_THROW(
            packageFileMeta.GetRelativeFilePath(
                    "abc/def", "abc/non_exist/package"),
            misc::InconsistentStateException);
}

vector<FileNodePtr> PackageFileMetaTest::InitFileNodes(
        const string& fileNodesDespStr, const string& packageFilePath)
{
    vector<FileNodePtr> fileNodes;
    vector<vector<string> > nodeInfos;
    // path:opentype:length:offset:dataIdx
    StringUtil::fromString(fileNodesDespStr, nodeInfos, ":", "#");

    for (size_t i = 0; i < nodeInfos.size(); ++i) 
    {
        string dirPath = PathUtil::GetParentDirPath(packageFilePath);
        string absPath = PathUtil::JoinPath(dirPath, nodeInfos[i][0]);
        FSFileType type = nodeInfos[i][1] == "DIR" ? 
                          FSFT_DIRECTORY : FSFT_IN_MEM;
        size_t length = StringUtil::numberFromString<size_t>(nodeInfos[i][2]);
        fileNodes.push_back(
            FileNodePtr(new FakeFileNode(absPath, length, type)));
    }
    return fileNodes;
}

void PackageFileMetaTest::CheckInnerFileMeta(const string& fileNodesDespStr, 
        const PackageFileMeta& packageFileMeta)
{
    vector<vector<string> > nodeInfos;
    StringUtil::fromString(fileNodesDespStr, nodeInfos, ":", "#");
    ASSERT_EQ(nodeInfos.size(), packageFileMeta.GetInnerFileCount());
    PackageFileMeta::Iterator iter;
    size_t i = 0;
    for (iter = packageFileMeta.Begin(); 
         iter != packageFileMeta.End(); ++iter, ++i)
    {
        assert(nodeInfos[i].size() == 5);
        ASSERT_EQ(nodeInfos[i][0], iter->GetFilePath());
        if (nodeInfos[i][1] == "DIR")
        {
            ASSERT_TRUE(iter->IsDir());
        }
        else
        {
            assert(nodeInfos[i][1] == "FILE");
            ASSERT_FALSE(iter->IsDir());
        }
        ASSERT_EQ(StringUtil::numberFromString<size_t>
                  (nodeInfos[i][2]), iter->GetLength());
        ASSERT_EQ(StringUtil::numberFromString<size_t>
                  (nodeInfos[i][3]), iter->GetOffset());
        ASSERT_EQ(StringUtil::numberFromString<size_t>
                  (nodeInfos[i][4]), iter->GetDataFileIdx());
    }
}

// path:opentype:length:offset:dataIdx#path:opentype:length:offset:dataIdx
void PackageFileMetaTest::InnerTestInitFromFileNodes(
        const string& fileNodesDespStr, size_t fileAlignSize)
{
    string packageFilePath = PathUtil::JoinPath(GET_TEST_DATA_PATH(),
            "package_file");
    vector<FileNodePtr> fileNodes = InitFileNodes(
            fileNodesDespStr, packageFilePath);
    PackageFileMeta packageFileMeta;
    packageFileMeta.InitFromFileNode(packageFilePath, 
            fileNodes, fileAlignSize);
    CheckInnerFileMeta(fileNodesDespStr, packageFileMeta);
}

void PackageFileMetaTest::TestGetPackageDataFileIdx()
{
    ASSERT_EQ(0, PackageFileMeta::GetPackageDataFileIdx("package_file.__data__0"));
    ASSERT_EQ(-1, PackageFileMeta::GetPackageDataFileIdx("package_file.__data__x"));
    ASSERT_EQ(-1, PackageFileMeta::GetPackageDataFileIdx("package_file__data__0"));
    ASSERT_EQ(100, PackageFileMeta::GetPackageDataFileIdx("file.__data__100"));
}


IE_NAMESPACE_END(file_system);

