#include "indexlib/file_system/archive/ArchiveDirectory.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "unittest/unittest.h"

namespace indexlib::file_system {

class ArchiveDirectoryTest : public TESTBASE
{
public:
    ArchiveDirectoryTest() = default;
    ~ArchiveDirectoryTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<IDirectory> CreateArchiveDirectory();

private:
    std::shared_ptr<IFileSystem> _fs;
};

void ArchiveDirectoryTest::setUp() { _fs = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow(); }

void ArchiveDirectoryTest::tearDown() {}

std::shared_ptr<IDirectory> ArchiveDirectoryTest::CreateArchiveDirectory()
{
    return IDirectory::Get(_fs)->CreateArchiveDirectory("").GetOrThrow();
}

TEST_F(ArchiveDirectoryTest, testUsage)
{
    {
        auto dir = CreateArchiveDirectory();
        auto subDir = dir->MakeDirectory("dir", DirectoryOption()).GetOrThrow();
        auto writer = subDir->CreateFileWriter("a", WriterOption()).GetOrThrow();
        ASSERT_EQ(4, writer->Write("abcd", 4).GetOrThrow());
        ASSERT_EQ(2, writer->Write("ef", 2).GetOrThrow());
        ASSERT_EQ(6, writer->GetLength());
        writer->Close().GetOrThrow();
        ASSERT_EQ(6, writer->GetLength());
        dir->Close().GetOrThrow();
    }
    {
        auto dir = CreateArchiveDirectory();
        auto subDir = dir->GetDirectory("dir").GetOrThrow();
        auto reader = subDir->CreateFileReader("a", ReaderOption(FSOT_BUFFERED)).GetOrThrow();
        char* buffer = new char[100];
        ASSERT_EQ(4, reader->Read(buffer, 4, 1).GetOrThrow());
        ASSERT_EQ("bcde", std::string(buffer, 4));

        delete[] buffer;
    }
}

TEST_F(ArchiveDirectoryTest, testDirectory)
{
    {
        auto dir = CreateArchiveDirectory();
        dir->MakeDirectory("a/b/c", DirectoryOption()).GetOrThrow();
        ASSERT_TRUE(dir->GetDirectory("a").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/c").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/c/").GetOrThrow());
        dir->Close().GetOrThrow();
    }
    {
        auto dir = CreateArchiveDirectory();
        std::vector<std::string> fileList;
        dir->ListDir("", ListOption::Recursive(), fileList).GetOrThrow();
        ASSERT_TRUE(dir->GetDirectory("a").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/c").GetOrThrow());
        ASSERT_TRUE(dir->GetDirectory("a/b/c/").GetOrThrow());
    }
}

TEST_F(ArchiveDirectoryTest, testListDir)
{
    {
        auto dir = CreateArchiveDirectory();
        dir->MakeDirectory("c/d/e", DirectoryOption()).GetOrThrow();
        dir->MakeDirectory("cc/e", DirectoryOption()).GetOrThrow();
        dir->CreateFileWriter("d", WriterOption()).GetOrThrow()->Close().GetOrThrow();
        dir->CreateFileWriter("b", WriterOption()).GetOrThrow()->Close().GetOrThrow();

        auto subDir = dir->GetDirectory("c/d").GetOrThrow();
        subDir->CreateFileWriter("d", WriterOption()).GetOrThrow()->Close().GetOrThrow();
        subDir->CreateFileWriter("f", WriterOption()).GetOrThrow()->Close().GetOrThrow();

        std::vector<std::string> fileList;
        std::vector<std::string> expectFileList = {"b", "c", "c/d", "c/d/d", "c/d/e", "c/d/f", "cc", "cc/e", "d"};
        dir->ListDir("", ListOption::Recursive(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);
        for (const std::string& path : expectFileList) {
            EXPECT_TRUE(dir->IsExist(path).GetOrThrow());
        }

        fileList.clear();
        expectFileList = {"b", "c", "cc", "d"};
        dir->ListDir("", ListOption(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);

        fileList.clear();
        expectFileList = {"d", "d/d", "d/e", "d/f"};
        dir->GetDirectory("c").GetOrThrow()->ListDir("", ListOption::Recursive(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);

        fileList.clear();
        expectFileList = {"d"};
        dir->GetDirectory("c").GetOrThrow()->ListDir("", ListOption(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);
    }
    {
        auto dir = CreateArchiveDirectory();
        dir->MakeDirectory("c", DirectoryOption()).GetOrThrow()->MakeDirectory("g", DirectoryOption()).GetOrThrow();
        std::vector<std::string> fileList;
        std::vector<std::string> expectFileList = {"b",     "c",   "c/d", "c/d/d", "c/d/e",
                                                   "c/d/f", "c/g", "cc",  "cc/e",  "d"};
        dir->ListDir("", ListOption::Recursive(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);
        for (const std::string& path : expectFileList) {
            EXPECT_TRUE(dir->IsExist(path).GetOrThrow());
        }

        fileList.clear();
        expectFileList = {"b", "c", "cc", "d"};
        dir->ListDir("", ListOption(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);

        fileList.clear();
        expectFileList = {"d", "d/d", "d/e", "d/f", "g"};
        dir->GetDirectory("c").GetOrThrow()->ListDir("", ListOption::Recursive(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);

        fileList.clear();
        expectFileList = {"d", "g"};
        dir->GetDirectory("c").GetOrThrow()->ListDir("", ListOption(), fileList).GetOrThrow();
        ASSERT_EQ(expectFileList, fileList);
    }
}

} // namespace indexlib::file_system
