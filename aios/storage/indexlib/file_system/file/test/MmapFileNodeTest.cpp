#include "indexlib/file_system/file/MmapFileNode.h"

#include "gtest/gtest.h"
#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/MmapFileNodeCreator.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class NonExistException;
class OutOfRangeException;
class UnSupportedException;
}} // namespace indexlib::util

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class MmapFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    MmapFileNodeTest();
    ~MmapFileNodeTest();

    DECLARE_CLASS_NAME(MmapFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMmapFileNode();
    void TestCaseForOpen();
    void TestCaseForRead();
    void TestCaseForReadWrite();
    void TestCaseForReadEmptyFile();
    void TestCaseForLock();
    void TestCaseForWrite();
    void TestCaseForCloseBeforeOpen();

private:
    void MakeData(const std::string& filePath, std::vector<uint8_t>& answer);
    std::shared_ptr<MmapFileNode> CreateFileNode(const std::string& filePath, FSOpenType type);
    void Check(std::shared_ptr<FileNode> fileNode, uint8_t* answer, uint32_t stepLen);

private:
    std::string _rootDir;
    LoadConfig _loadConfig;
    util::BlockMemoryQuotaControllerPtr _memoryController;
    static const uint32_t TOTAL_BYTE = 1000;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MmapFileNodeTest);

INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestMmapFileNode);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForOpen);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForReadWrite);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForReadEmptyFile);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForLock);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForCloseBeforeOpen);
//////////////////////////////////////////////////////////////////////

MmapFileNodeTest::MmapFileNodeTest() {}

MmapFileNodeTest::~MmapFileNodeTest() {}

void MmapFileNodeTest::CaseSetUp()
{
    _rootDir = util::PathUtil::NormalizePath(GET_TEMP_DATA_PATH()) + "/";
    LoadConfig::FilePatternStringVector patterns;
    patterns.push_back(".*");
    _loadConfig.SetFilePatternString(patterns);
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MmapFileNodeTest::CaseTearDown() {}

void MmapFileNodeTest::TestMmapFileNode()
{
    std::shared_ptr<MmapFileNode> mmapFileNode;
    FileSystemTestUtil::CreateDiskFile(_rootDir + "file", "disk file");

    LoadConfig loadConfig3 = LoadConfigListCreator::MakeMmapLoadConfig(false, true, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig3, _memoryController, false));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Open("LOGICAL_PATH", _rootDir + "file", FSOT_MMAP, -1));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Populate());
    ASSERT_TRUE(mmapFileNode->_warmup);
    ASSERT_EQ(FSOT_MMAP, mmapFileNode->GetOpenType());
    ASSERT_EQ(FSFT_MMAP_LOCK, mmapFileNode->_type);

    LoadConfig loadConfig4 = LoadConfigListCreator::MakeMmapLoadConfig(false, false, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig4, _memoryController, false));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Open("LOGICAL_PATH", _rootDir + "file", FSOT_MMAP, -1));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Populate());
    ASSERT_FALSE(mmapFileNode->_warmup);
    ASSERT_EQ(FSOT_MMAP, mmapFileNode->GetOpenType());
    ASSERT_EQ(FSFT_MMAP, mmapFileNode->_type);
}

void MmapFileNodeTest::TestCaseForOpen()
{
    std::shared_ptr<MmapFileNode> mmapFileNode;
    FileSystemTestUtil::CreateDiskFile(_rootDir + "existFile", "disk file");

    LoadConfig loadConfig2 = LoadConfigListCreator::MakeMmapLoadConfig(true, true, false, 1024, 0);
    mmapFileNode.reset(new MmapFileNode(loadConfig2, _memoryController, false));
    ASSERT_EQ(FSEC_NOENT, mmapFileNode->Open("LOGICAL_PATH", _rootDir + "notExistFile", FSOT_MMAP, -1));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Open("LOGICAL_PATH", _rootDir + "existFile", FSOT_MMAP, -1));
    ASSERT_EQ(FSEC_OK, mmapFileNode->Populate());
    ASSERT_EQ(FSEC_OK, mmapFileNode->Close());
}

void MmapFileNodeTest::TestCaseForRead()
{
    string filePath = _rootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    std::shared_ptr<MmapFileNode> fileNode = CreateFileNode(filePath, FSOT_MMAP);
    ASSERT_EQ(FSFT_MMAP, fileNode->GetType());
    ASSERT_EQ(answer.size(), fileNode->GetLength());
    ASSERT_TRUE(fileNode->GetBaseAddress());
    ASSERT_FALSE(fileNode->_warmup);

    Check(fileNode, (uint8_t*)answer.data(), 1);
    Check(fileNode, (uint8_t*)answer.data(), 23);
    Check(fileNode, (uint8_t*)answer.data(), 200);
    Check(fileNode, (uint8_t*)answer.data(), TOTAL_BYTE);

    char buffer[TOTAL_BYTE + 1];
    ASSERT_EQ(std::make_pair(FSEC_BADARGS, 0ul), fileNode->Read(buffer, TOTAL_BYTE + 1, 0, ReadOption()).CodeWith());
    ASSERT_EQ(nullptr, fileNode->ReadToByteSliceList(TOTAL_BYTE + 1, 0, ReadOption()));
}

void MmapFileNodeTest::TestCaseForReadWrite()
{
    string filePath = _rootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    std::shared_ptr<MmapFileNode> fileNode = CreateFileNode(filePath, FSOT_MMAP);
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
    string filePath = _rootDir + "data";
    FileSystemTestUtil::CreateDiskFile(filePath, "");

    std::shared_ptr<MmapFileNode> fileNode = CreateFileNode(filePath, FSOT_MMAP);
    ASSERT_EQ((size_t)0, fileNode->GetLength());
    ASSERT_TRUE(fileNode->GetBaseAddress() == NULL);
}

void MmapFileNodeTest::TestCaseForLock()
{
    string filePath = _rootDir + "data";
    vector<uint8_t> answer;
    answer.resize(TOTAL_BYTE);
    MakeData(filePath, answer);

    {
        _loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, true, false, 10 * 1024 * 1024, 10);
        std::shared_ptr<MmapFileNode> fileNode = CreateFileNode(filePath, FSOT_MMAP);
        ASSERT_TRUE(fileNode->_warmup);
    }
    {
        _loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(true, true);
        std::shared_ptr<MmapFileNode> fileNode = CreateFileNode(filePath, FSOT_MMAP);
        ASSERT_TRUE(fileNode->_warmup);
    }
}

void MmapFileNodeTest::TestCaseForWrite()
{
    LoadConfig loadConfig = LoadConfigListCreator::MakeMmapLoadConfig(false, false, false, 1024, 0);
    MmapFileNode mmapFile(loadConfig, _memoryController, false);
    uint8_t buffer[] = "abc";
    ASSERT_EQ(FSEC_NOTSUP, mmapFile.Write(buffer, 3).Code());
}

void MmapFileNodeTest::MakeData(const string& filePath, vector<uint8_t>& answer)
{
    fslib::fs::File* file = fslib::fs::FileSystem::openFile(filePath, fslib::WRITE);
    for (uint32_t i = 0; i < answer.size(); i++) {
        uint8_t ch = (uint8_t)i % 8;
        file->write((void*)(&ch), sizeof(ch));
        answer[i] = ch;
    }
    file->close();
    delete file;
}

std::shared_ptr<MmapFileNode> MmapFileNodeTest::CreateFileNode(const string& filePath, FSOpenType type)
{
    MmapFileNodeCreator creator;
    creator.Init(_loadConfig, _memoryController);
    std::shared_ptr<FileNode> fileNode = creator.CreateFileNode(type, false, "");
    EXPECT_EQ(FSEC_OK, fileNode->Open(filePath, filePath, type, -1));
    EXPECT_EQ(FSEC_OK, fileNode->Populate());
    std::shared_ptr<MmapFileNode> mmapFileNode = std::dynamic_pointer_cast<MmapFileNode>(fileNode);
    assert(mmapFileNode);
    return mmapFileNode;
}

void MmapFileNodeTest::Check(std::shared_ptr<FileNode> fileNode, uint8_t* answer, uint32_t stepLen)
{
    uint32_t readTimes = TOTAL_BYTE / stepLen;
    for (uint32_t i = 0; i < readTimes; i++) {
        uint32_t len = stepLen;
        if (readTimes > 1 && i == readTimes - 1) {
            len = TOTAL_BYTE % stepLen;
        }

        ByteSliceList* byteSliceList = fileNode->ReadToByteSliceList(len, i * stepLen, ReadOption());
        ASSERT_TRUE(byteSliceList != NULL);
        ByteSlice* byteSlice = byteSliceList->GetHead();
        ASSERT_TRUE(byteSlice != NULL);
        ASSERT_EQ(0, memcmp(byteSlice->data, answer + i * stepLen, len));
        byteSliceList->Clear(NULL);
        delete byteSliceList;

        uint8_t buffer2[TOTAL_BYTE];
        ASSERT_EQ(FSEC_OK, fileNode->Read((void*)buffer2, len, i * stepLen, ReadOption()).Code());
        ASSERT_EQ(0, memcmp(buffer2, answer + i * stepLen, len));
    }
}

void MmapFileNodeTest::TestCaseForCloseBeforeOpen()
{
    LoadConfig loadConfig;
    std::shared_ptr<MmapFileNode> mmapFileNode(new MmapFileNode(loadConfig, _memoryController, false));
    ASSERT_NO_THROW(mmapFileNode.reset());
}
}} // namespace indexlib::file_system
