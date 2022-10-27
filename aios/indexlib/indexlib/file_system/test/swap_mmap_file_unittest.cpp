#include "indexlib/file_system/test/swap_mmap_file_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SwapMmapFileTest);

SwapMmapFileTest::SwapMmapFileTest()
{
}

SwapMmapFileTest::~SwapMmapFileTest()
{
}

void SwapMmapFileTest::CaseSetUp()
{
}

void SwapMmapFileTest::CaseTearDown()
{
}

void SwapMmapFileTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    InnerTestSimpleProcess(true);    
    InnerTestSimpleProcess(false);
}

void SwapMmapFileTest::InnerTestSimpleProcess(bool autoStore)
{
    TearDown();
    SetUp();
    
    DirectoryPtr segDir = GET_SEGMENT_DIRECTORY();

    // create file
    SwapMmapFileWriterPtr smFileWriter = segDir->CreateSwapMmapFileWriter("swap_mmap_file", 1024);
    ASSERT_TRUE(smFileWriter);

    // write file
    string content = "abc";
    ASSERT_EQ(size_t(3), smFileWriter->Write(content.c_str(), 3));

    // read file
    SwapMmapFileReaderPtr smFileReader = segDir->CreateSwapMmapFileReader("swap_mmap_file");
    char* baseAddr = (char*)smFileReader->GetBaseAddress();
    ASSERT_EQ(content, string(baseAddr, 3));
    ASSERT_EQ((size_t)3, smFileReader->GetLength());

    // get & read file
    SwapMmapFileReaderPtr smFileReader1 = segDir->CreateSwapMmapFileReader("swap_mmap_file");
    ASSERT_TRUE(smFileReader1);
    baseAddr[0] = 'b';
    char* newBaseAddr = (char*)smFileReader1->GetBaseAddress();
    ASSERT_EQ(string("bbc"), string(newBaseAddr, 3));

    // sync & truncate file
    smFileWriter->Sync();

    string loadStr;
    string filePath = segDir->GetPath() + "/swap_mmap_file";
    FileSystemWrapper::AtomicLoad(filePath, loadStr);
    ASSERT_EQ(string("bbc"), loadStr.substr(0, 3));

    if (autoStore)
    {
        smFileWriter->Close();
    }
    
    smFileWriter.reset();
    smFileReader.reset();
    smFileReader1.reset();
    
    segDir->GetFileSystem()->CleanCache();
    ASSERT_EQ(autoStore, FileSystemWrapper::IsExist(filePath));
}

IE_NAMESPACE_END(file_system);

