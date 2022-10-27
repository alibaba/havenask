#include "indexlib/file_system/test/pack_directory_unittest.h"
#include "indexlib/file_system/package_file_writer.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackDirectoryTest);

namespace
{
class MockPackageFileWriter : public PackageFileWriter
{
public:
    MockPackageFileWriter(const std::string& filePath)
        : PackageFileWriter(filePath)
    {}

    MOCK_METHOD2(CreateInnerFileWriter, FileWriterPtr(
                     const std::string& filePath, const FSWriterParam& param));
    MOCK_METHOD1(MakeInnerDir, void(const std::string& dirPath));
    MOCK_CONST_METHOD1(GetPhysicalFiles, void(std::vector<std::string>& files));
    MOCK_METHOD0(DoClose, void());
};
}

PackDirectoryTest::PackDirectoryTest()
{
}

PackDirectoryTest::~PackDirectoryTest()
{
}

void PackDirectoryTest::CaseSetUp()
{
}

void PackDirectoryTest::CaseTearDown()
{
}

void PackDirectoryTest::TestInit()
{
    DirectoryPtr inMemDir = 
        GET_PARTITION_DIRECTORY()->MakeInMemDirectory("in_mem");
    PackageFileWriterPtr packageFileWriter = 
        inMemDir->CreatePackageFileWriter("pack_file");

    {
        // package file not in inMemDir
        PackDirectory directory(GET_TEST_DATA_PATH(), GET_FILE_SYSTEM());
        ASSERT_THROW(directory.Init(packageFileWriter), BadParameterException);
    }

    {
        // package file in inMemDir
        PackDirectory directory(inMemDir->GetPath(), GET_FILE_SYSTEM());
        ASSERT_NO_THROW(directory.Init(packageFileWriter));
        ASSERT_TRUE(directory.mPackageFileWriter);
    }

    {
        // directory in package file
        string subDir = PathUtil::JoinPath(inMemDir->GetPath(), "subDir");
        PackDirectory directory(subDir, GET_FILE_SYSTEM());
        ASSERT_NO_THROW(directory.Init(packageFileWriter));
        ASSERT_TRUE(directory.mPackageFileWriter);
    }
}

void PackDirectoryTest::TestCreateFileWriter()
{
    DirectoryPtr inMemDir = 
        GET_PARTITION_DIRECTORY()->MakeInMemDirectory("in_mem");

    string packageFilePath = PathUtil::JoinPath(
            inMemDir->GetPath(), "package_file");
    MockPackageFileWriter* mockWriter = 
        new MockPackageFileWriter(packageFilePath);

    PackageFileWriterPtr packageFileWriter(mockWriter);

    {    
        // package file in inMemDir
        PackDirectory directory(inMemDir->GetPath(), GET_FILE_SYSTEM());
        directory.Init(packageFileWriter);

        FileWriterPtr retInnerWriter;
        EXPECT_CALL(*mockWriter, CreateInnerFileWriter("abc", _))
            .WillOnce(Return(retInnerWriter));
        directory.CreateFileWriter("abc");
    }

    {
        // sub dir in package file 
        string subDirPath = PathUtil::JoinPath(inMemDir->GetPath(), "subDir");
        PackDirectory directory(subDirPath, GET_FILE_SYSTEM());
        directory.Init(packageFileWriter);

        FileWriterPtr retInnerWriter;
        EXPECT_CALL(*mockWriter, CreateInnerFileWriter("subDir/abc", _))
            .WillOnce(Return(retInnerWriter));
        directory.CreateFileWriter("abc");
    }
}

void PackDirectoryTest::TestMakeDirectory()
{
    DirectoryPtr inMemDir = 
        GET_PARTITION_DIRECTORY()->MakeInMemDirectory("in_mem");

    string packageFilePath = PathUtil::JoinPath(
            inMemDir->GetPath(), "package_file");
    MockPackageFileWriter* mockWriter = 
        new MockPackageFileWriter(packageFilePath);

    PackageFileWriterPtr packageFileWriter(mockWriter);
    {    
        // package file in inMemDir
        PackDirectory directory(inMemDir->GetPath(), GET_FILE_SYSTEM());
        directory.Init(packageFileWriter);

        EXPECT_CALL(*mockWriter, MakeInnerDir("abc")).Times(1);
        DirectoryPtr retDir = directory.MakeDirectory("abc");
        ASSERT_TRUE(DYNAMIC_POINTER_CAST(PackDirectory, retDir));
    }

    {
        // sub dir in package file 
        string subDirPath = PathUtil::JoinPath(inMemDir->GetPath(), "subDir");
        PackDirectory directory(subDirPath, GET_FILE_SYSTEM());
        directory.Init(packageFileWriter);

        EXPECT_CALL(*mockWriter, MakeInnerDir("subDir/abc")).Times(1);
        DirectoryPtr retDir = directory.MakeDirectory("abc");
        ASSERT_TRUE(DYNAMIC_POINTER_CAST(PackDirectory, retDir));
    }
}

void PackDirectoryTest::TestCreateInMemoryFile()
{
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDir = partDir->MakeInMemDirectory("in_mem");
    PackDirectoryPtr packDir = inMemDir->CreatePackDirectory("package_file");
    DirectoryPtr innerDir = packDir->MakeDirectory("inner_dir");
    InMemoryFilePtr inMemFile = innerDir->CreateInMemoryFile("inner_file", 4);

    char* addr = (char*)inMemFile->GetBaseAddress();
    memcpy(addr, "abc", 4);
    inMemFile->Sync();
    packDir->ClosePackageFileWriter();
    packDir->Sync(true);

    FileSystemWrapper::SymLink(GET_TEST_DATA_PATH() + "/in_mem",
                               GET_TEST_DATA_PATH() + "/link_dir");

    DirectoryPtr linkDir = partDir->GetDirectory("link_dir", true);
    assert(linkDir);

    ASSERT_FALSE(linkDir->IsExist("inner_dir"));
    ASSERT_FALSE(linkDir->IsExist("inner_dir/inner_file"));
    
    linkDir->MountPackageFile("package_file");
    ASSERT_TRUE(linkDir->IsExist("inner_dir"));
    ASSERT_TRUE(linkDir->IsExist("inner_dir/inner_file"));

    string fileContent;
    linkDir->Load("inner_dir/inner_file", fileContent);
    ASSERT_EQ(string("abc", 4), fileContent);
}

IE_NAMESPACE_END(file_system);

