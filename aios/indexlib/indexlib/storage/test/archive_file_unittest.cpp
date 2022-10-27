#include "indexlib/storage/test/archive_file_unittest.h"

using namespace std;


IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ArchiveFileTest);

ArchiveFileTest::ArchiveFileTest()
{
}

ArchiveFileTest::~ArchiveFileTest()
{
}

void ArchiveFileTest::CaseSetUp()
{
}

void ArchiveFileTest::CaseTearDown()
{
}

void ArchiveFileTest::TestRead()
{
    for (size_t i = 1; i < 12; i++)
    {
        InnerTestRead(i);
    }
}

void ArchiveFileTest::InnerTestRead(size_t writeBufferSize)
{
    TearDown();
    SetUp();
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    ArchiveFilePtr archiveFile = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test1", fslib::WRITE));
    archiveFile->ResetBufferSize(writeBufferSize);
    string content = "0123456789";
    archiveFile->Write(content.c_str(), content.size());
    archiveFile->Close();
    archiveFolderWrite.Close();
    
    //test read
    ArchiveFolder archiveFolder(false);
    archiveFolder.Open(GET_TEST_DATA_PATH());
    ArchiveFilePtr readFile = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolder.GetInnerFile(
                                     "test1", fslib::READ));
    ASSERT_EQ(writeBufferSize, readFile->GetBufferSize());
    char buffer[10];
    for (size_t readLen = 1; readLen < 12; readLen++)
    {
        for (size_t beginPos = 0; beginPos < 12; beginPos++)
        {
            size_t len = readFile->PRead(buffer, readLen, beginPos);
            CheckResult(content, beginPos, readLen, len, buffer);
        }
    }
    archiveFolder.Close();
}

void ArchiveFileTest::CheckResult(
        const string& content,
        size_t beginPos, size_t readLen,
        size_t actualReadLen, char* readResult)
{
    if (beginPos >= content.size())
    {
        ASSERT_EQ(0u, actualReadLen);
        return;
    }
    size_t leftLen = content.size() - beginPos;
    size_t expectReadLen = leftLen > readLen ? readLen : leftLen;
    ASSERT_EQ(expectReadLen, actualReadLen);
    for (size_t i = 0; i < expectReadLen; i++)
    {
        ASSERT_EQ(readResult[i], content[beginPos + i]);
    }
}

void ArchiveFileTest::TestRewrite()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    //write hello world and close
    FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content1 = "hello world";
    archiveFile->Write(content1.c_str(), content1.size());
    archiveFile->Close();
    //write nihao and not close
    archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content2 = "nihao";
    archiveFile->Write(content2.c_str(), content2.size());
    archiveFile->Flush();
    //check read file get "hello world" not "nihao"
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH(), "1");
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    string fileContent;
    FileSystemWrapper::AtomicLoad(archiveFile.get(), fileContent);
    ASSERT_EQ(content1, fileContent);
    //write hello kitty and close
    archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content3 = "hello kitty";
    archiveFile->Write(content3.c_str(), content3.size());
    archiveFile->Close();
    //last read folder can not read new file
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    FileSystemWrapper::AtomicLoad(archiveFile.get(), fileContent);
    ASSERT_EQ(content1, fileContent);
    archiveFolderRead.Close();
    //prepare new read folder
    archiveFolderRead.Open(GET_TEST_DATA_PATH(), "1");
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    FileSystemWrapper::AtomicLoad(archiveFile.get(), fileContent);
    ASSERT_EQ(content3, fileContent);
    archiveFolderRead.Close();
    archiveFolderWrite.Close();
}

void ArchiveFileTest::TestGetFileLength()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    ArchiveFilePtr archiveFile = DYNAMIC_POINTER_CAST(ArchiveFile, 
            archiveFolderWrite.GetInnerFile("test1", fslib::WRITE));
    string content1 = "hello world";
    archiveFile->Write(content1.c_str(), content1.size());
    archiveFile->Close();
    ASSERT_EQ(content1.size(), archiveFile->GetFileLength());

    //empty file
    content1 = "";
    archiveFile.reset();
    archiveFile = DYNAMIC_POINTER_CAST(ArchiveFile, 
        archiveFolderWrite.GetInnerFile("test1", fslib::WRITE));
    archiveFile->Write(content1.c_str(), content1.size());
    archiveFile->Close();
    ASSERT_EQ(content1.size(), archiveFile->GetFileLength());
    archiveFolderWrite.Close();
}

void ArchiveFileTest::TestFlush()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    ArchiveFilePtr archiveFile1 = DYNAMIC_POINTER_CAST(ArchiveFile, 
        archiveFolderWrite.GetInnerFile("test1", fslib::WRITE));
    ArchiveFilePtr archiveFile2 = DYNAMIC_POINTER_CAST(ArchiveFile, 
        archiveFolderWrite.GetInnerFile("test2", fslib::WRITE));

    string content1 = "hello world1";
    string content2 = "hello World222";
    archiveFile2->Write(content2.c_str(), content2.size());
    archiveFile1->Write(content1.c_str(), content1.size());
    archiveFile1->Flush();

    string metaFileContent=R"(
30	0	12	test1_0_0_archive_file_block_0
)";
    
    string dataFilePath = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(), 
                                                      "INDEXLIB_ARCHIVE_FILES_1_0.lfd");
    string metaFilePath = FileSystemWrapper::JoinPath(GET_TEST_DATA_PATH(), 
                                                      "INDEXLIB_ARCHIVE_FILES_1_0.lfm");
    string realContent;
    FileWrapperPtr file(FileSystemWrapper::OpenFile(dataFilePath, fslib::READ));
    FileWrapperPtr metaFile(FileSystemWrapper::OpenFile(metaFilePath, fslib::READ));
    FileSystemWrapper::AtomicLoad(file.get(), realContent);
    ASSERT_EQ(content1, realContent);
    realContent.clear();
    FileSystemWrapper::AtomicLoad(metaFile.get(), realContent);
    ASSERT_EQ(metaFileContent, realContent);
    archiveFile2->Flush();
    realContent.clear();
    FileSystemWrapper::AtomicLoad(file.get(), realContent);
    ASSERT_EQ(content1 + content2, realContent);
    metaFileContent = R"(
30	0	12	test1_0_0_archive_file_block_0
30	12	14	test2_0_1_archive_file_block_0
)";
    realContent.clear();
    FileSystemWrapper::AtomicLoad(metaFile.get(), realContent);
    ASSERT_EQ(metaFileContent, realContent);

    file->Close();
    metaFile->Close();
    archiveFolderWrite.Close();
}

IE_NAMESPACE_END(storage);

