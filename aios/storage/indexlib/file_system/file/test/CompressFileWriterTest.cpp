#include "indexlib/file_system/file/CompressFileWriter.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <vector>

#include "autil/Thread.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LinkDirectory.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/DecompressCachedCompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/NormalCompressFileReader.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class CompressFileWriterTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    CompressFileWriterTest();
    ~CompressFileWriterTest();

    DECLARE_CLASS_NAME(CompressFileWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestIsMemoryDirectory();
    void TestIsNotMemoryDirectory();
    void TestIsDecompressedCacheLoadDirectory();

    void TestUseRootLink();
    void TestCompressAddressMapperMemoryStatistic();
    void TestReadInMemByteSliceList();
    void TestReadNormalBlockByteSliceList();
    void TestReadDecompressCachedBlockByteSliceList();
    void TestReadMultiplDecompressCachedBlockByteSliceList();
    void TestInvalidFilePath();
    void TestEnableHintDataForNonZstd();
    void TestUseZstdHintDataByReference();
    void TestOpenInThreadOwnFs();
    void TestSwitchLoadType();

private:
    void InnerTestReadDecompressCachedBlockByteSliceList(uint32_t compressBufferSize);
    void InnerTestSimpleProcess(const std::shared_ptr<Directory>& dir, const DirectoryPtr& dirForCheck = nullptr);
    std::string MakeOriginData(size_t dataLen, bool randomData);

    void CheckData(const std::string& oriDataStr, const CompressFileReaderPtr& compressReader, size_t readStep);

    void ExtractSliceInfo(util::ByteSliceList* sliceList, uint32_t& sliceCount, size_t& totalSize, size_t& curDataSize)
    {
        assert(sliceList);
        sliceCount = 0;
        totalSize = 0;
        curDataSize = 0;

        util::ByteSlice* slice = sliceList->GetHead();
        while (slice) {
            ++sliceCount;
            totalSize += slice->size;
            curDataSize += slice->dataSize;
            slice = slice->next;
        }
    }

    void CheckSliceList(util::ByteSliceList* sliceList, uint32_t expectSliceCount, size_t expectTotalSize,
                        size_t expectCurDataSize)
    {
        uint32_t sliceCount = 0;
        size_t totalSize = 0;
        size_t curDataSize = 0;
        ExtractSliceInfo(sliceList, sliceCount, totalSize, curDataSize);

        ASSERT_EQ(expectSliceCount, sliceCount);
        ASSERT_EQ(expectTotalSize, totalSize);
        ASSERT_EQ(expectCurDataSize, curDataSize);
    }

    std::shared_ptr<FileWriter> CreateFileWriter(const std::shared_ptr<Directory>& dir, const std::string& path,
                                                 size_t blockSize)
    {
        if (_enableHint) {
            WriterOption option = WriterOption::Compress("zstd", blockSize);
            option.compressorParams["enable_hint_data"] = "true";
            option.compressorParams["hint_sample_block_count"] = "64";
            option.compressorParams["hint_sample_ratio"] = "0.1";
            option.compressorParams["hint_always_use_hint"] = "true";
            if (random() % 2 == 1) {
                option.compressorParams["enable_meta_file"] = "true";
            }
            return dir->CreateFileWriter(path, option);
        }
        return dir->CreateFileWriter(path, WriterOption::Compress("lz4", blockSize));
    }

private:
    AUTIL_LOG_DECLARE();
    bool _enableHint;
    std::shared_ptr<IFileSystem> _fileSystem;
    std::shared_ptr<Directory> _directory;
};
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileWriterTest);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(CompressFileWriterTestMode, CompressFileWriterTest, testing::Values(false, true));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestIsMemoryDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestIsNotMemoryDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestIsDecompressedCacheLoadDirectory);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestUseRootLink);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestCompressAddressMapperMemoryStatistic);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestReadInMemByteSliceList);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestReadNormalBlockByteSliceList);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestReadDecompressCachedBlockByteSliceList);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestReadMultiplDecompressCachedBlockByteSliceList);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestInvalidFilePath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestEnableHintDataForNonZstd);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestUseZstdHintDataByReference);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestOpenInThreadOwnFs);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(CompressFileWriterTestMode, TestSwitchLoadType);

//////////////////////////////////////////////////////////////////////

CompressFileWriterTest::CompressFileWriterTest() {}

CompressFileWriterTest::~CompressFileWriterTest() {}

void CompressFileWriterTest::CaseSetUp()
{
    _enableHint = GET_CASE_PARAM();
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void CompressFileWriterTest::CaseTearDown() {}

void CompressFileWriterTest::TestIsMemoryDirectory()
{
    std::shared_ptr<Directory> dir = _directory->MakeDirectory("test_segment", DirectoryOption::Mem());
    InnerTestSimpleProcess(dir);
}

void CompressFileWriterTest::TestIsNotMemoryDirectory() { InnerTestSimpleProcess(_directory); }

void CompressFileWriterTest::TestIsDecompressedCacheLoadDirectory()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "cache_decompress_file" : true
            }
        }
        ]
    })";

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
    InnerTestSimpleProcess(_directory);
}

void CompressFileWriterTest::TestSwitchLoadType()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "cache_decompress_file" : true
            }
        }
        ]
    })";

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    auto dir = _directory;
    string filePath = "tmp";
    WriterOption option = WriterOption::Compress("lz4", 1024);
    option.compressorParams["enable_hint_data"] = "true";
    option.compressorParams["hint_sample_block_count"] = "64";
    option.compressorParams["hint_sample_ratio"] = "0.1";
    option.compressorParams["hint_always_use_hint"] = "true";
    std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(filePath, option);
    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, fileWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    char* readBuffer = new char[dataLen + 1];

    CompressFileReaderPtr compressReader1 = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP)));

    {
        size_t decompressLen = compressReader1->Read(readBuffer, dataLen, ReadOption()).GetOrThrow();
        ASSERT_EQ(dataLen, decompressLen);

        string decompressData(readBuffer, dataLen);
        ASSERT_TRUE(oriData == decompressData);
    }

    CompressFileReaderPtr compressReader2 = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_CACHE)));
    {
        size_t decompressLen = compressReader2->Read(readBuffer, dataLen, ReadOption()).GetOrThrow();
        ASSERT_EQ(dataLen, decompressLen);

        string decompressData(readBuffer, dataLen);
        ASSERT_TRUE(oriData == decompressData);
    }
    delete[] readBuffer;
}

void CompressFileWriterTest::TestUseZstdHintDataByReference()
{
    auto dir = _directory;
    size_t strLen = 17 * 1024 * 1024;
    string org;
    org.reserve(strLen);
    for (size_t i = 0; i < strLen; i += 32) {
        org.append(32, (char)((i * 13) % 128));
    }

    string filePath = "tmp";
    WriterOption option = WriterOption::Compress("zstd", 4096);
    option.compressorParams["enable_hint_data"] = "true";
    option.compressorParams["hint_sample_block_count"] = "1024";
    option.compressorParams["hint_sample_ratio"] = "0.1";
    option.compressorParams["hint_always_use_hint"] = "true";
    std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(filePath, option);
    ASSERT_EQ(FSEC_OK, fileWriter->Write(org.data(), org.size()).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MEM)));
    CheckData(org, compressReader, 4096);
    CompressFileReaderPtr compressReader1 = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP)));
    CheckData(org, compressReader1, 4096);

    auto addressMapper = compressReader->GetCompressFileAddressMapper();
    auto addressMapper1 = compressReader1->GetCompressFileAddressMapper();
    ASSERT_EQ(addressMapper->GetBlockCount(), addressMapper1->GetBlockCount());
    for (int64_t i = 0; i < addressMapper->GetBlockCount(); i++) {
        ASSERT_EQ(addressMapper->IsCompressBlockUseHintData(i), addressMapper1->IsCompressBlockUseHintData(i));
        if (!addressMapper->IsCompressBlockUseHintData(i)) {
            continue;
        }
        auto hintData = addressMapper->GetHintData(i);
        auto hintData1 = addressMapper1->GetHintData(i);
        ASSERT_TRUE(hintData != nullptr);
        ASSERT_TRUE(hintData1 != nullptr);
        ASSERT_NE(hintData, hintData1);
    }
}

void CompressFileWriterTest::TestEnableHintDataForNonZstd()
{
    auto dir = _directory;
    string filePath = "tmp";
    WriterOption option = WriterOption::Compress("snappy", 1024);
    option.compressorParams["enable_hint_data"] = "true";
    option.compressorParams["hint_sample_block_count"] = "64";
    option.compressorParams["hint_sample_ratio"] = "0.1";
    std::shared_ptr<FileWriter> fileWriter = dir->CreateFileWriter(filePath, option);

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, fileWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP)));
    CheckData(oriData, compressReader, 1024);

    // check data length with no hint compressor
    string cmpFilePath = "cmp";
    fileWriter = dir->CreateFileWriter(cmpFilePath, WriterOption::Compress("snappy", 1024));
    ASSERT_EQ(FSEC_OK, fileWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // check compress info & check hint block count = 0;
    std::shared_ptr<CompressFileInfo> compressInfo = dir->GetCompressFileInfo(filePath);
    ASSERT_EQ(string("0"), compressInfo->additionalInfo["use_hint_block_count"]);
    ASSERT_EQ(string("0"), compressInfo->additionalInfo["use_hint_saved_size"]);
    ASSERT_TRUE(!compressInfo->additionalInfo["lib_version"].empty());

    bool useMetaFile = indexlib::util::GetTypeValueFromKeyValueMap(
        compressInfo->additionalInfo, COMPRESS_ENABLE_META_FILE, DEFAULT_COMPRESS_ENABLE_META_FILE);
    if (!useMetaFile) {
        size_t dataLenWithHint = dir->GetFileLength(filePath);
        size_t dataLenForCmp = dir->GetFileLength(cmpFilePath);
        ASSERT_EQ(dataLenWithHint,
                  dataLenForCmp + sizeof(size_t) * 3); // hint header is 3 * sizeof(size_t), no hint block meta
    }
    // check resource file
    size_t resourceLen = dir->GetFileLength(filePath + ".compress_resource_mmap");
    ASSERT_GT(resourceLen, 0);
    ASSERT_EQ(1, _fileSystem->GetFileSystemMetrics().GetOutputStorageMetrics().GetFileCount(FSMG_LOCAL, FSFT_RESOURCE));
}

void CompressFileWriterTest::InnerTestSimpleProcess(const std::shared_ptr<Directory>& dir,
                                                    const DirectoryPtr& dirForCheck)
{
    string filePath = "tmp";
    CompressFileWriterPtr compressWriter =
        std::dynamic_pointer_cast<CompressFileWriter>(CreateFileWriter(dir, filePath, 1024));
    std::shared_ptr<FileWriter> fileWriter = compressWriter->GetDataFileWriter();

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(fileWriter->GetLogicalPath(), compressWriter->GetLogicalPath());
    ASSERT_EQ(fileWriter->GetLength(), compressWriter->GetLength());
    ASSERT_LT(fileWriter->GetLength(), compressWriter->GetLogicLength());
    ASSERT_TRUE(compressWriter->GetLength() < dataLen);
    ASSERT_EQ(FSEC_OK, compressWriter->Close());

    CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(
        (dirForCheck ? dirForCheck : dir)->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG)));
    std::shared_ptr<FileReader> fileReader = compressReader->GetDataFileReader();

    ASSERT_EQ(fileReader->GetLength(), compressReader->GetLength());

    char* readBuffer = new char[dataLen + 1];
    size_t decompressLen = compressReader->Read(readBuffer, dataLen, ReadOption()).GetOrThrow();
    ASSERT_EQ(dataLen, decompressLen);

    string decompressData(readBuffer, dataLen);
    ASSERT_TRUE(oriData == decompressData);
    delete[] readBuffer;

    vector<autil::ThreadPtr> threads;
    const size_t THREAD_COUNT = 5;
    for (size_t i = 0; i < THREAD_COUNT; i++) {
        autil::ThreadPtr thread = autil::Thread::createThread(std::bind([&]() {
            CheckData(oriData, compressReader, 1);
            CheckData(oriData, compressReader, 10);
            CheckData(oriData, compressReader, 128);
            CheckData(oriData, compressReader, 512 + 1);
            CheckData(oriData, compressReader, 1024);
            CheckData(oriData, compressReader, 2 * 1024);
        }));
        threads.emplace_back(thread);
    }
    for (auto& thread : threads) {
        thread->join();
    }
}

void CompressFileWriterTest::TestReadInMemByteSliceList()
{
    std::shared_ptr<Directory> segDir = _directory;
    string filePath = "tmp";
    std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, 8192);

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, compressWriter->Close());

    std::shared_ptr<FileReader> compressReader =
        segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP));
    util::ByteSliceList* byteSliceList = compressReader->ReadToByteSliceList(dataLen, 0, ReadOption());
    CheckSliceList(byteSliceList, 1, dataLen, 0);

    string ret;
    ret.resize(dataLen);
    ByteSliceReader sliceReader;
    sliceReader.Open(byteSliceList).GetOrThrow();
    CheckSliceList(byteSliceList, 1, dataLen, 8192);

    ASSERT_EQ(dataLen, sliceReader.Read((void*)ret.data(), ret.size()).GetOrThrow());
    ASSERT_EQ(oriData, ret);
    CheckSliceList(byteSliceList, 10, dataLen, dataLen);
    delete byteSliceList;
}

void CompressFileWriterTest::TestInvalidFilePath()
{
    std::shared_ptr<Directory> segDir = _directory;
    string filePath = "test.__compress_info__";
    ASSERT_THROW(CreateFileWriter(segDir, filePath, 4096), util::UnSupportedException);

    filePath = "test.__compress_meta__";
    ASSERT_THROW(CreateFileWriter(segDir, filePath, 4096), util::UnSupportedException);

    filePath = "test.__compress_info__a";
    std::shared_ptr<FileWriter> writer = CreateFileWriter(segDir, filePath, 4096);
    ASSERT_EQ(FSEC_OK, writer->Close());

    ASSERT_TRUE(segDir->IsExist(filePath + COMPRESS_FILE_INFO_SUFFIX));

    std::shared_ptr<FileReader> compressReader =
        segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP));
    ASSERT_EQ(0, compressReader->GetLogicLength());
    if (segDir->IsExist(filePath + COMPRESS_FILE_META_SUFFIX)) {
        ASSERT_EQ(0, compressReader->GetLength());
    } else {
        ASSERT_NE(0, compressReader->GetLength());
    }
}

void CompressFileWriterTest::TestReadNormalBlockByteSliceList()
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    std::shared_ptr<Directory> segDir = _directory;
    string filePath = "tmp";
    std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, 4096);

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, compressWriter->Close());

    std::shared_ptr<FileReader> compressReader =
        segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_CACHE));
    NormalCompressFileReaderPtr typedReader = std::dynamic_pointer_cast<NormalCompressFileReader>(compressReader);
    std::shared_ptr<FileNode> fileNode = typedReader->GetDataFileReader()->GetFileNode();
    std::shared_ptr<BlockFileNode> blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(fileNode);
    assert(blockFileNode);
    util::BlockCache* blockCache = blockFileNode->GetBlockCache();
    assert(blockCache);

    util::ByteSliceList* byteSliceList = compressReader->ReadToByteSliceList(dataLen, 0, ReadOption());
    CheckSliceList(byteSliceList, 1, dataLen, 0);

    string ret;
    ret.resize(dataLen);
    ByteSliceReader sliceReader;
    sliceReader.Open(byteSliceList).GetOrThrow();
    CheckSliceList(byteSliceList, 1, dataLen, 4096);

    ASSERT_EQ(dataLen, sliceReader.Read((void*)ret.data(), ret.size()).GetOrThrow());
    ASSERT_EQ(oriData, ret);
    CheckSliceList(byteSliceList, 19, dataLen, dataLen);
    ASSERT_LT(blockCache->GetResourceInfo().memoryUse, dataLen);
    delete byteSliceList;
}

void CompressFileWriterTest::TestReadMultiplDecompressCachedBlockByteSliceList()
{
    InnerTestReadDecompressCachedBlockByteSliceList(2048);
}

void CompressFileWriterTest::TestReadDecompressCachedBlockByteSliceList()
{
    InnerTestReadDecompressCachedBlockByteSliceList(4096);
}

void CompressFileWriterTest::InnerTestReadDecompressCachedBlockByteSliceList(uint32_t compressBufferSize)
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "cache_decompress_file" : true
            }
        }
        ]
    })";

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    std::shared_ptr<Directory> segDir = _directory;
    string filePath = "tmp";
    std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, compressBufferSize);

    size_t dataLen = 73 * 1024; // 73KB
    string oriData = MakeOriginData(dataLen, !_enableHint);
    ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
    ASSERT_EQ(FSEC_OK, compressWriter->Close());

    std::shared_ptr<FileReader> compressReader =
        segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_CACHE));
    DecompressCachedCompressFileReaderPtr typedReader =
        std::dynamic_pointer_cast<DecompressCachedCompressFileReader>(compressReader);
    assert(compressReader);
    util::BlockCache* blockCache = typedReader->GetBlockCache();
    assert(blockCache);

    util::ByteSliceList* byteSliceList = compressReader->ReadToByteSliceList(dataLen, 0, ReadOption());
    CheckSliceList(byteSliceList, 1, dataLen, 0);

    string ret;
    ret.resize(dataLen);
    ByteSliceReader sliceReader;
    sliceReader.Open(byteSliceList).GetOrThrow();
    CheckSliceList(byteSliceList, 1, dataLen, compressBufferSize);

    ASSERT_EQ(dataLen, sliceReader.Read((void*)ret.data(), ret.size()).GetOrThrow());
    ASSERT_EQ(oriData, ret);

    size_t expectSliceCount = (dataLen + compressBufferSize - 1) / compressBufferSize;
    CheckSliceList(byteSliceList, expectSliceCount, dataLen, dataLen);
    ASSERT_GT(blockCache->GetResourceInfo().memoryUse, dataLen);
    delete byteSliceList;
}

void CompressFileWriterTest::TestUseRootLink()
{
    FileSystemOptions fsOptions;
    fsOptions.useRootLink = true;
    std::shared_ptr<IFileSystem> fs =
        FileSystemCreator::Create("TestUseRootLink", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    std::shared_ptr<Directory> rootDirectory = Directory::Get(fs);
    InnerTestSimpleProcess(rootDirectory, rootDirectory->CreateLinkDirectory());
}

void CompressFileWriterTest::TestOpenInThreadOwnFs()
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = true;
    options.needFlush = true;
    options.useRootLink = false;
    options.isOffline = true;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    _enableHint = false;
    string filePath = "merge_tmp";
    {
        // prepare data
        std::shared_ptr<Directory> segDir = _directory;
        std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, 128);

        string oriData = MakeOriginData(4096, true);
        ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
        ASSERT_EQ(FSEC_OK, compressWriter->Close());
    }

    auto baseFs = _fileSystem;
    std::shared_ptr<IFileSystem> fs1(std::move(baseFs->CreateThreadOwnFileSystem("thread_1").GetOrThrow()));
    std::shared_ptr<IFileSystem> fs2(std::move(baseFs->CreateThreadOwnFileSystem("thread_2").GetOrThrow()));
    std::shared_ptr<Directory> dir1 = Directory::Get(std::dynamic_pointer_cast<IFileSystem>(fs1));
    std::shared_ptr<Directory> dir2 = Directory::Get(std::dynamic_pointer_cast<IFileSystem>(fs2));
    std::shared_ptr<FileReader> reader1 = dir1->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_CACHE));
    std::shared_ptr<FileReader> reader2 = dir2->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_CACHE));
}

void CompressFileWriterTest::TestCompressAddressMapperMemoryStatistic()
{
    // TODO: qingran: metrics
    return;
    {
        std::shared_ptr<Directory> segDir = _directory;
        string filePath = "tmp";
        std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, 128);

        size_t dataLen = 73 * 1024; // 73KB
        string oriData = MakeOriginData(dataLen, !_enableHint);
        ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
        ASSERT_EQ(FSEC_OK, compressWriter->Close());

        CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(
            segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_MMAP)));

        _fileSystem->CleanCache();
        const StorageMetrics& localStorageMetrics = _fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
        size_t expectLen = compressReader->GetLength();
        ASSERT_EQ(expectLen, localStorageMetrics.GetTotalFileLength());
    }
    tearDown();
    setUp();
    {
        std::shared_ptr<Directory> segDir = _directory;
        string filePath = "tmp";
        std::shared_ptr<FileWriter> compressWriter = CreateFileWriter(segDir, filePath, 128);

        size_t dataLen = 73 * 1024; // 73KB
        string oriData = MakeOriginData(dataLen, !_enableHint);
        ASSERT_EQ(FSEC_OK, compressWriter->Write(oriData.data(), oriData.size()).Code());
        ASSERT_EQ(FSEC_OK, compressWriter->Close());

        CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(
            segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_BUFFERED)));

        _fileSystem->CleanCache();
        const StorageMetrics& localStorageMetrics = _fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();

        const std::shared_ptr<CompressFileInfo>& compressInfo = compressReader->GetCompressInfo();
        size_t addressMapperLen = (compressInfo->blockCount + 1) * sizeof(size_t);
        size_t expectLen = compressReader->GetLength() + addressMapperLen;
        ASSERT_EQ(expectLen, localStorageMetrics.GetTotalFileLength());

        CompressFileReaderPtr compressReaderExist = std::dynamic_pointer_cast<CompressFileReader>(
            segDir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_BUFFERED)));
        _fileSystem->CleanCache();
        const StorageMetrics& localStorageMetricsExist = _fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
        const std::shared_ptr<CompressFileInfo>& compressInfoExist = compressReaderExist->GetCompressInfo();
        size_t addressMapperLenExist = (compressInfoExist->blockCount + 1) * sizeof(size_t);
        size_t expectLenExist = compressReaderExist->GetLength() + addressMapperLenExist;
        ASSERT_EQ(expectLenExist, localStorageMetricsExist.GetTotalFileLength());
    }
}

string CompressFileWriterTest::MakeOriginData(size_t dataLen, bool randomData)
{
    if (!randomData && dataLen < 8 * 1024 * 1024) {
        string testDataPath = GET_PRIVATE_TEST_DATA_PATH() + "data";
        FILE* fp = fopen(testDataPath.c_str(), "r");
        string ret;
        ret.resize(dataLen);
        size_t readLen = fread((void*)ret.data(), sizeof(char), dataLen, fp);
        assert(dataLen == readLen);
        (void)readLen;
        fclose(fp);
        return ret;
    }

    string orgData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        orgData[i] = (char)(i * 3 % 127);
    }
    return orgData;
}

void CompressFileWriterTest::CheckData(const string& oriDataStr, const CompressFileReaderPtr& compressReader,
                                       size_t readStep)
{
    autil::mem_pool::Pool pool;
    CompressFileReader* sessionReader = compressReader->CreateSessionReader(&pool);
    vector<char> buffer(readStep);

    size_t lastOffset = 0;
    string lastExpectStr;
    for (size_t i = 0; i < oriDataStr.size(); i += readStep) {
        string expectStr = oriDataStr.substr(i, readStep);
        size_t readLen = sessionReader->Read(buffer.data(), readStep, i, ReadOption()).GetOrThrow();
        ASSERT_TRUE(expectStr.compare(0, readLen, buffer.data(), readLen) == 0) << i;

        if (i > 0) {
            // read backward
            readLen = sessionReader->Read(buffer.data(), readStep, lastOffset, ReadOption()).GetOrThrow();
            ASSERT_TRUE(lastExpectStr.compare(0, readLen, buffer.data(), readLen) == 0) << i;
        }
        lastExpectStr = expectStr;
        lastOffset = i;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(&pool, sessionReader);
}
}} // namespace indexlib::file_system
