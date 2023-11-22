#include "indexlib/file_system/file/CompressFileAddressMapper.h"

#include <autil/Thread.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class CompressFileAddressMapperTest : public INDEXLIB_TESTBASE
{
public:
    CompressFileAddressMapperTest();
    ~CompressFileAddressMapperTest();

    DECLARE_CLASS_NAME(CompressFileAddressMapperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestEstimateMemUsed();
    void TestMultiThreadInitForRead();

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressFileAddressMapperTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CompressFileAddressMapperTest, TestEstimateMemUsed);
INDEXLIB_UNIT_TEST_CASE(CompressFileAddressMapperTest, TestMultiThreadInitForRead);

AUTIL_LOG_SETUP(indexlib.file_system, CompressFileAddressMapperTest);

CompressFileAddressMapperTest::CompressFileAddressMapperTest() {}

CompressFileAddressMapperTest::~CompressFileAddressMapperTest() {}

void CompressFileAddressMapperTest::CaseSetUp()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache"
        }
        ]
    })";

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = true;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void CompressFileAddressMapperTest::CaseTearDown() {}

void CompressFileAddressMapperTest::TestSimpleProcess()
{
    auto TestDumpAndRead = [&](const vector<size_t>& blockLengthVec, const vector<bool>& useHintVec,
                               bool enableCompress, FSOpenType openType, bool expectEncoded,
                               bool expectMapDataInResFile, bool expectHintBitmapInResFile,
                               size_t addressMapperBatchNum) {
        CaseTearDown();
        CaseSetUp();
        size_t blockSize = 4096;
        assert(blockLengthVec.size() == useHintVec.size());
        CompressFileAddressMapper mapper;
        ASSERT_EQ(FSEC_OK, mapper.InitForWrite(blockSize, addressMapperBatchNum));
        for (size_t i = 0; i < blockLengthVec.size(); i++) {
            mapper.AddOneBlock(blockLengthVec[i], useHintVec[i]);
        }

        std::shared_ptr<Directory> dir = _directory;
        string fileName = "tmp_data";
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        dir->RemoveFile(fileName, removeOption);
        std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(fileName);
        size_t compressLen = mapper.GetCompressFileLength();
        string tmpStr(compressLen, 'a');
        ASSERT_EQ(FSEC_OK, fileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
        mapper.Dump(fileWriter, enableCompress);
        ASSERT_EQ(FSEC_OK, fileWriter->Close());

        std::shared_ptr<CompressFileInfo> fileInfo(new CompressFileInfo);
        fileInfo->blockCount = blockLengthVec.size();
        fileInfo->blockSize = blockSize;
        fileInfo->compressFileLen = compressLen;
        fileInfo->deCompressFileLen = blockLengthVec.size() * blockSize;
        fileInfo->enableCompressAddressMapper = enableCompress;

        if (addressMapperBatchNum != 0) {
            fileInfo->additionalInfo["address_mapper_batch_number"] =
                autil::StringUtil::toString(addressMapperBatchNum);
        }
        fileInfo->additionalInfo["enable_meta_file"] = "false";
        if (mapper.GetUseHintBlockCount() > 0) {
            fileInfo->additionalInfo["use_hint_block_count"] =
                autil::StringUtil::toString(mapper.GetUseHintBlockCount());
        }

        std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(openType));
        CompressFileAddressMapper loadMapper;
        loadMapper.InitForRead(fileInfo, fileReader, dir->GetIDirectory().get());
        ASSERT_EQ(expectEncoded, loadMapper._isCompressed);
        if (expectEncoded) {
            ASSERT_NE((blockLengthVec.size() + 1) * sizeof(size_t), loadMapper.GetAddressMapperDataSize());
        } else {
            ASSERT_EQ((blockLengthVec.size() + 1) * sizeof(size_t), loadMapper.GetAddressMapperDataSize());
        }

        size_t maxBlockSize = 0;
        size_t offset = 0;
        for (size_t i = 0; i < blockLengthVec.size(); i++) {
            ASSERT_EQ(offset, loadMapper.CompressBlockAddress(i));
            ASSERT_EQ(blockLengthVec[i], loadMapper.CompressBlockLength(i));
            offset += blockLengthVec[i];
            maxBlockSize = max(maxBlockSize, blockLengthVec[i]);
            ASSERT_EQ(useHintVec[i], loadMapper.IsCompressBlockUseHintData(i));
        }
        ASSERT_EQ(maxBlockSize, loadMapper.GetMaxCompressBlockSize());

        ResourceFilePtr resFile = loadMapper._resourceFile;
        if (expectHintBitmapInResFile || expectMapDataInResFile) {
            ASSERT_TRUE(resFile != nullptr);
            ASSERT_EQ(expectMapDataInResFile,
                      resFile->GetResource<CompressFileAddressMapper::CompressMapperResource>()->mapperBaseAddress !=
                          nullptr);
            ASSERT_EQ(expectHintBitmapInResFile,
                      resFile->GetResource<CompressFileAddressMapper::CompressMapperResource>()->hintBitmapData !=
                          nullptr);
        } else {
            ASSERT_TRUE(resFile == nullptr);
        }
    };

    vector<size_t> blockLengthVec = {1023, 1214, 1267, 1235, 1236, 1234, 1239, 2012};
    vector<bool> useHintVec = {false, false, false, false, false, false, false, false};

    // no compress & in memory
    bool enableCompress = false;
    FSOpenType openType = FSOT_MEM;
    bool expectEncoded = false;
    bool expectMapDataInResFile = false;
    bool expectHintBitmapInResFile = false;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 0);

    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 16);

    // no compress & block cache
    enableCompress = false;
    openType = FSOT_CACHE;
    expectEncoded = false;
    expectMapDataInResFile = true;
    expectHintBitmapInResFile = false;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 8);

    // compress & block cache
    openType = FSOT_CACHE;
    enableCompress = true;
    expectEncoded = true;
    expectMapDataInResFile = true;
    expectHintBitmapInResFile = false;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 10);

    // compress & in memory
    openType = FSOT_MEM;
    enableCompress = true;
    expectEncoded = true;
    expectMapDataInResFile = false;
    expectHintBitmapInResFile = false;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 7);

    // no compress & has hint block & inMem
    openType = FSOT_MEM;
    enableCompress = false;
    expectEncoded = false;
    expectMapDataInResFile = false;
    expectHintBitmapInResFile = false;
    useHintVec[0] = true;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 9);

    // compress & has hint block & inMem
    openType = FSOT_MEM;
    enableCompress = true;
    expectEncoded = true;
    expectMapDataInResFile = false;
    expectHintBitmapInResFile = false;
    useHintVec[0] = true;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 6);

    // no compress & has hint block & cache
    openType = FSOT_CACHE;
    enableCompress = false;
    expectEncoded = false;
    expectMapDataInResFile = true;
    expectHintBitmapInResFile = false;
    useHintVec[0] = true;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 5);

    // compress & has hint block & cache
    openType = FSOT_CACHE;
    enableCompress = true;
    expectEncoded = true;
    expectMapDataInResFile = true;
    expectHintBitmapInResFile = true;
    useHintVec[0] = true;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 11);

    // encode fail, use no encode format
    blockLengthVec[7] = 66236;
    openType = FSOT_MEM;
    enableCompress = true;
    expectEncoded = false;
    expectMapDataInResFile = false;
    expectHintBitmapInResFile = false;
    useHintVec[0] = true;
    TestDumpAndRead(blockLengthVec, useHintVec, enableCompress, openType, expectEncoded, expectMapDataInResFile,
                    expectHintBitmapInResFile, 12);
}

void CompressFileAddressMapperTest::TestEstimateMemUsed()
{
    std::shared_ptr<Directory> dir = _directory;
    string fileName = "tmp_data";
    ASSERT_EQ(0, CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                            indexlib::file_system::FSOT_LOAD_CONFIG));
    ASSERT_EQ(0, CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                            indexlib::file_system::FSOT_MEM_ACCESS));

    auto writeOption = WriterOption::Compress("lz4", 4096);
    writeOption.compressorParams["encode_address_mapper"] = "true";
    writeOption.compressorParams["address_mapper_batch_number"] = 16;
    writeOption.compressorParams["enable_meta_file"] = "false";
    writeOption.compressorParams["enable_hint_data"] = "true";
    writeOption.compressorParams["hint_sample_ratio"] = "0.1";
    std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(fileName, writeOption);
    string tmpStr(10000, 'a');
    ASSERT_EQ(FSEC_OK, fileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    auto compressInfo = _directory->GetCompressFileInfo(fileName);
    ASSERT_TRUE(compressInfo);
    size_t hintDataSize = GetTypeValueFromKeyValueMap(compressInfo->additionalInfo, "hint_data_size", (size_t)0);
    size_t addressDataSize =
        GetTypeValueFromKeyValueMap(compressInfo->additionalInfo, "address_mapper_data_size", (size_t)0);
    ASSERT_EQ(0, CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                            indexlib::file_system::FSOT_MEM_ACCESS));
    ASSERT_EQ(hintDataSize + addressDataSize,
              CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                         indexlib::file_system::FSOT_LOAD_CONFIG));

    {
        std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(FSOT_CACHE));
        CompressFileAddressMapper loadMapper;
        loadMapper.InitForRead(compressInfo, fileReader, dir->GetIDirectory().get());
        ASSERT_EQ(true, loadMapper._isCompressed);
        ASSERT_LE(hintDataSize + addressDataSize, loadMapper.EvaluateCurrentMemUsed());
    }
    ASSERT_EQ(0, CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                            indexlib::file_system::FSOT_LOAD_CONFIG));
    {
        std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(FSOT_MEM));
        CompressFileAddressMapper loadMapper;
        loadMapper.InitForRead(compressInfo, fileReader, dir->GetIDirectory().get());
        ASSERT_EQ(true, loadMapper._isCompressed);
        ASSERT_LE(hintDataSize, loadMapper.EvaluateCurrentMemUsed());
    }

    ASSERT_EQ(0, CompressFileAddressMapper::EstimateMemUsed(dir->GetIDirectory(), fileName,
                                                            indexlib::file_system::FSOT_LOAD_CONFIG));
    {
        std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(FSOT_CACHE));
        CompressFileAddressMapper loadMapper;
        loadMapper.InitForRead(compressInfo, fileReader, dir->GetIDirectory().get());
        ASSERT_EQ(true, loadMapper._isCompressed);
        ASSERT_EQ(0, loadMapper.EvaluateCurrentMemUsed());
    }
    {
        std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(FSOT_MEM));
        CompressFileAddressMapper loadMapper;
        loadMapper.InitForRead(compressInfo, fileReader, dir->GetIDirectory().get());
        ASSERT_EQ(true, loadMapper._isCompressed);
        ASSERT_EQ(0, loadMapper.EvaluateCurrentMemUsed());
    }
}

void CompressFileAddressMapperTest::TestMultiThreadInitForRead()
{
    std::shared_ptr<Directory> dir = _directory;
    string fileNamePrefix = "tmp_data_";
    auto writeOption = WriterOption::Compress("lz4", 4096);
    writeOption.compressorParams["encode_address_mapper"] = "true";
    writeOption.compressorParams["address_mapper_batch_number"] = 16;
    writeOption.compressorParams["enable_meta_file"] = "false";
    writeOption.compressorParams["enable_hint_data"] = "true";
    writeOption.compressorParams["hint_sample_ratio"] = "0.1";

    size_t fileCount = 10;
    for (size_t fileId = 0; fileId < fileCount; fileId++) {
        string fileName = fileNamePrefix + autil::StringUtil::toString(fileId);
        std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(fileName, writeOption);
        string tmpStr(10000, 'a');
        ASSERT_EQ(FSEC_OK, fileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
        ASSERT_EQ(FSEC_OK, fileWriter->Close());
    }
    std::function<void()> readFiles = [fileNamePrefix, fileCount, dir]() {
        for (size_t fileId = 0; fileId < fileCount; fileId++) {
            string fileName = fileNamePrefix + autil::StringUtil::toString(fileId);
            auto compressInfo = dir->GetCompressFileInfo(fileName);
            std::shared_ptr<FileReader> fileReader = dir->CreateFileReader(fileName, ReaderOption(FSOT_CACHE));
            CompressFileAddressMapper loadMapper;
            loadMapper.InitForRead(compressInfo, fileReader, dir->GetIDirectory().get());
            ASSERT_EQ(true, loadMapper._isCompressed);
        }
    };
    size_t readThreadCount = 5;
    vector<autil::ThreadPtr> readThreads;
    for (size_t threadId = 0; threadId < readThreadCount; threadId++) {
        string threadName = "ReadThread_" + autil::StringUtil::toString(threadId);
        readThreads.push_back(autil::Thread::createThread(readFiles, threadName));
    }
}

}} // namespace indexlib::file_system
