#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/file_system/mmap_file_node_creator.h"
#include "indexlib/file_system/test/mmap_file_node_unittest.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MmapFileNodeTest);

MmapFileNodeTest::MmapFileNodeTest()
{
}

MmapFileNodeTest::~MmapFileNodeTest()
{
}

void MmapFileNodeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    mLoadConfig.SetFilePatternString(patterns);
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MmapFileNodeTest::CaseTearDown()
{
}

void MmapFileNodeTest::TestMmapFileNode()
{
    MmapFileNodePtr mmapFileNode;
    FileSystemTestUtil::CreateDiskFile(mRootDir + "file", "disk file");

    LoadConfig loadConfig3 = LoadConfigListCreator::MakeMmapLoadConfig(false, true, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig3, mMemoryController, false));
    mmapFileNode->Open(mRootDir + "file", FSOT_MMAP);
    mmapFileNode->Populate();
    ASSERT_TRUE(mmapFileNode->mWarmup);
    ASSERT_EQ(FSOT_MMAP, mmapFileNode->GetOpenType());
    ASSERT_EQ(FSFT_MMAP_LOCK, mmapFileNode->mType);

    LoadConfig loadConfig4 = LoadConfigListCreator::MakeMmapLoadConfig(false, false, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig4, mMemoryController, false));
    mmapFileNode->Open(mRootDir + "file", FSOT_MMAP);
    mmapFileNode->Populate();
    ASSERT_FALSE(mmapFileNode->mWarmup);
    ASSERT_EQ(FSOT_MMAP, mmapFileNode->GetOpenType());
    ASSERT_EQ(FSFT_MMAP, mmapFileNode->mType);
}

void MmapFileNodeTest::TestCaseForOpen()
{
    MmapFileNodePtr mmapFileNode;
    FileSystemTestUtil::CreateDiskFile(mRootDir + "existFile", "disk file");

    LoadConfig loadConfig2 = LoadConfigListCreator::MakeMmapLoadConfig(true, true, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig2, mMemoryController, false));
    ASSERT_THROW(mmapFileNode->Open(mRootDir + "notExistFile", FSOT_MMAP),
                 NonExistException);
    ASSERT_NO_THROW(mmapFileNode->Open(mRootDir + "existFile", FSOT_MMAP));
    mmapFileNode->Populate();
    mmapFileNode->Close();
}

void MmapFileNodeTest::TestCaseForRead()
{
    string filePath = mRootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
    ASSERT_EQ(FSFT_MMAP, fileNode->GetType());
    ASSERT_EQ(answer.size(), fileNode->GetLength());
    ASSERT_TRUE(fileNode->GetBaseAddress()); 
    ASSERT_FALSE(fileNode->mWarmup); 
    
    Check(fileNode, (uint8_t*)answer.data(), 1);
    Check(fileNode, (uint8_t*)answer.data(), 23);
    Check(fileNode, (uint8_t*)answer.data(), 200);
    Check(fileNode, (uint8_t*)answer.data(), TOTAL_BYTE);

    ASSERT_THROW(fileNode->Read(TOTAL_BYTE + 1, 0), OutOfRangeException);
}

void MmapFileNodeTest::TestCaseForReadWrite()
{
    string filePath = mRootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
    ASSERT_EQ(FSFT_MMAP, fileNode->GetType());
    ASSERT_EQ(answer.size(), fileNode->GetLength());

    uint8_t* data = (uint8_t*)fileNode->GetBaseAddress();
    data[500] = 2 * data[500];
    answer[500] = 2 * answer[500];

    Check(fileNode, (uint8_t*)answer.data(), 1);
    Check(fileNode, (uint8_t*)answer.data(), 23);
    Check(fileNode, (uint8_t*)answer.data(), 200);
    Check(fileNode, (uint8_t*)answer.data(), TOTAL_BYTE);
}

void MmapFileNodeTest::TestCaseForReadEmptyFile() 
{
    string filePath = mRootDir + "data";
    FileSystemTestUtil::CreateDiskFile(filePath, "");    

    MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
    INDEXLIB_TEST_EQUAL((size_t)0, fileNode->GetLength());
    INDEXLIB_TEST_TRUE(fileNode->GetBaseAddress() == NULL);
}

void MmapFileNodeTest::TestCaseForLock()
{
    string filePath = mRootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    {
        mLoadConfig = LoadConfigListCreator::MakeMmapLoadConfig(
                false, true, false, 10 * 1024 * 1024, 10);
        MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
        ASSERT_TRUE(fileNode->mWarmup); 
    }
    {
        mLoadConfig = LoadConfigListCreator::MakeMmapLoadConfig(true, true);
        MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
        ASSERT_TRUE(fileNode->mWarmup); 
    }
    {
        mLoadConfig = LoadConfigListCreator::MakeMmapLoadConfig(
                false, false, false, 4 * 1024 * 1024, 0, true);
        MmapFileNodePtr fileNode = CreateFileNode(filePath, FSOT_MMAP);
        fileNode->Lock(1, 2);
        fileNode->Lock(1, 2);
        ASSERT_FALSE(fileNode->mWarmup);
    }
}

void MmapFileNodeTest::TestCaseForWrite()
{
    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, false, false, 1024, 0);
    MmapFileNode mmapFile(loadConfig, mMemoryController, false);
    uint8_t buffer[] = "abc";
    ASSERT_THROW(mmapFile.Write(buffer, 3), UnSupportedException);
}

void MmapFileNodeTest::MakeData(const string& filePath, vector<uint8_t>& answer)
{
    File* file = FileSystem::openFile(filePath, WRITE);
    for (uint32_t i = 0; i < answer.size(); i++)
    {
        uint8_t ch = (uint8_t)i % 8;
        file->write((void*)(&ch), sizeof(ch));
        answer[i] = ch;
    }
    file->close();
    delete file;
}

MmapFileNodePtr MmapFileNodeTest::CreateFileNode(const string& filePath,
        FSOpenType type)
{
    MmapFileNodeCreator creator;
    creator.Init(mLoadConfig, mMemoryController);
    FileNodePtr fileNode = creator.CreateFileNode(filePath, type, false);
    fileNode->Open(filePath, type);
    fileNode->Populate();
    MmapFileNodePtr mmapFileNode = DYNAMIC_POINTER_CAST(
            MmapFileNode, fileNode);
    assert(mmapFileNode);
    return mmapFileNode;
}

void MmapFileNodeTest::Check(FileNodePtr fileNode, uint8_t* answer, 
                             uint32_t stepLen)
{
    uint32_t readTimes =  TOTAL_BYTE / stepLen;
    for (uint32_t i = 0; i < readTimes; i++)
    {
        uint32_t len = stepLen;
        if (readTimes > 1 && i == readTimes - 1)
        {
            len = TOTAL_BYTE % stepLen;
        }

        ByteSliceList* byteSliceList = fileNode->Read(len, i * stepLen);
        ASSERT_TRUE(byteSliceList != NULL);
        ByteSlice* byteSlice = byteSliceList->GetHead();
        ASSERT_TRUE(byteSlice != NULL);
        ASSERT_EQ(0, memcmp(byteSlice->data, answer + i * stepLen, len));
        byteSliceList->Clear(NULL);
        delete byteSliceList;

        uint8_t buffer2[TOTAL_BYTE];
        fileNode->Read((void*)buffer2, len, i * stepLen);
        ASSERT_EQ(0, memcmp(buffer2, answer + i * stepLen, len));
    }
}

void MmapFileNodeTest::TestCaseForCloseBeforeOpen()
{
    LoadConfig loadConfig;
    MmapFileNodePtr mmapFileNode(new MmapFileNode(loadConfig, mMemoryController, false));
    ASSERT_NO_THROW(mmapFileNode.reset());
}

IE_NAMESPACE_END(file_system);

