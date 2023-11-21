#include "indexlib/file_system/file/CompressFileReader.h"

#include "fslib/fs/ErrorGenerator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/DecompressCachedCompressFileReader.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class CompressFileReaderTest : public INDEXLIB_TESTBASE
{
public:
    CompressFileReaderTest();
    ~CompressFileReaderTest();

    DECLARE_CLASS_NAME(CompressFileReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBatchReadCacheCompressFile();
    void TestBatchReadBlockCache();
    void TestBatchReadMmap();
    void TestNoUselessDecompress();
    void TestException();

private:
    void PrepareFile(string& fileContent);
    void CheckData(const string& expectedData, void* buffer, size_t len, size_t offset);

    void InnerTest(string loadConfig);
    std::string MakeOriginData(size_t dataLen);
    std::shared_ptr<FileWriter> CreateFileWriter(const std::shared_ptr<Directory>& dir, const std::string& path,
                                                 size_t blockSize)
    {
        return dir->CreateFileWriter(path, WriterOption::Compress("lz4", blockSize));
    }

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;
    size_t _blockSize;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressFileReaderTest, TestBatchReadCacheCompressFile);
INDEXLIB_UNIT_TEST_CASE(CompressFileReaderTest, TestBatchReadBlockCache);
INDEXLIB_UNIT_TEST_CASE(CompressFileReaderTest, TestBatchReadMmap);
INDEXLIB_UNIT_TEST_CASE(CompressFileReaderTest, TestNoUselessDecompress);
INDEXLIB_UNIT_TEST_CASE(CompressFileReaderTest, TestException);
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileReaderTest);

CompressFileReaderTest::CompressFileReaderTest() { _blockSize = 4096; }

CompressFileReaderTest::~CompressFileReaderTest() {}

string CompressFileReaderTest::MakeOriginData(size_t dataLen)
{
    string orgData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        orgData[i] = (char)(i * 3 % 127);
    }
    return orgData;
}
void CompressFileReaderTest::CheckData(const string& expectedData, void* buffer, size_t len, size_t offset)
{
    ASSERT_TRUE(buffer);
    ASSERT_TRUE(offset + len <= expectedData.size());
    for (size_t i = 0; i < len; ++i) {
        ASSERT_EQ(expectedData[i + offset], ((char*)buffer)[i]);
    }
}
void CompressFileReaderTest::PrepareFile(string& fileContent)
{
    auto dir = _directory;
    string filePath = "tmp";
    CompressFileWriterPtr compressWriter =
        std::dynamic_pointer_cast<CompressFileWriter>(CreateFileWriter(dir, filePath, 4096));
    std::shared_ptr<FileWriter> fileWriter = compressWriter->GetDataFileWriter();

    size_t dataLen = 73 * 1024; // 73KB
    fileContent = MakeOriginData(dataLen);
    ASSERT_EQ(FSEC_OK, compressWriter->Write(fileContent.data(), fileContent.size()).Code());
    ASSERT_EQ(fileWriter->GetLogicalPath(), compressWriter->GetLogicalPath());
    ASSERT_EQ(fileWriter->GetLength(), compressWriter->GetLength());
    ASSERT_LT(fileWriter->GetLength(), compressWriter->GetLogicLength());
    ASSERT_TRUE(compressWriter->GetLength() < dataLen);
    ASSERT_EQ(FSEC_OK, compressWriter->Close());
}

void CompressFileReaderTest::CaseSetUp()
{
    _blockSize = 4096;
    fslib::fs::FileSystem::_useMock = false;
}

void CompressFileReaderTest::CaseTearDown() {}

void CompressFileReaderTest::InnerTest(string loadConfig)
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    string oriData;
    PrepareFile(oriData);
    auto dir = _directory;
    string filePath = "tmp";
    char buffer[100][8192];
    auto fileReader = dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG));
    {
        CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(fileReader);
        ASSERT_TRUE(compressReader);
    }

    {
        // try to read two continuous 4k
        BatchIO batchIO;
        batchIO.push_back(SingleIO(buffer[0], 4096, 0));
        batchIO.push_back(SingleIO(buffer[1], 4096, 4096));
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(2, result.size());
        ASSERT_TRUE(result[0].OK());
        ASSERT_EQ(4096, result[0].GetOrThrow());
        CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
        ASSERT_TRUE(result[1].OK());
        ASSERT_EQ(4096, result[1].GetOrThrow());
        CheckData(oriData, batchIO[1].buffer, batchIO[1].len, batchIO[1].offset);
    }

    {
        BatchIO batchIO;
        size_t fileLen = oriData.length();
        // test read out of bounds
        // offset valid, offset+len > fileLength
        batchIO.push_back(SingleIO(buffer[0], 1, fileLen - 1));
        // offset > fileLength
        batchIO.push_back(SingleIO(buffer[1], 200, fileLen - 1));
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(2, result.size());
        ASSERT_FALSE(result[0].OK());
        ASSERT_FALSE(result[1].OK());
    }
    {
        // read same block
        BatchIO batchIO;
        size_t fileLen = oriData.length();
        // test read out of bounds
        batchIO.push_back(SingleIO(buffer[0], 10, fileLen - 10));
        batchIO.push_back(SingleIO(buffer[1], 10, fileLen - 10));
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(2, result.size());
        ASSERT_TRUE(result[0].OK());
        ASSERT_TRUE(result[1].OK());
        ASSERT_EQ(10, result[0].GetOrThrow());
        ASSERT_EQ(10, result[1].GetOrThrow());
        CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
        CheckData(oriData, batchIO[1].buffer, batchIO[1].len, batchIO[1].offset);
    }
    {
        // read back
        BatchIO batchIO;
        batchIO.push_back(SingleIO(buffer[0], 4096, 0));
        batchIO.push_back(SingleIO(buffer[1], 4096, 4096));
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(2, result.size());
        ASSERT_TRUE(result[0].OK());
        ASSERT_EQ(4096, result[0].GetOrThrow());
        CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
        ASSERT_TRUE(result[1].OK());
        ASSERT_EQ(4096, result[1].GetOrThrow());
        CheckData(oriData, batchIO[1].buffer, batchIO[1].len, batchIO[1].offset);
    }
}

void CompressFileReaderTest::TestBatchReadCacheCompressFile()
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
    InnerTest(jsonStr);
}

void CompressFileReaderTest::TestBatchReadMmap()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "mmap"
        }
        ]
    })";
    InnerTest(jsonStr);
}
void CompressFileReaderTest::TestBatchReadBlockCache()
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
    InnerTest(jsonStr);
}

void CompressFileReaderTest::TestNoUselessDecompress()
{
    string loadConfig = R"(
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
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    string oriData;
    PrepareFile(oriData);
    auto dir = _directory;
    string filePath = "tmp";
    char buffer[100][8192];
    auto fileReader = dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG));
    auto compressReader = std::dynamic_pointer_cast<DecompressCachedCompressFileReader>(fileReader);
    compressReader->_batchSize = 2;

    ASSERT_TRUE(compressReader);
    {
        // decompress 2 time for 2 continues block
        BatchIO batchIO({{buffer[0], 100, 0}, {buffer[1], 200, 4096}});
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, option));
        ASSERT_EQ(100, result[0].GetOrThrow());
        ASSERT_EQ(200, result[1].GetOrThrow());
        CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
        CheckData(oriData, batchIO[1].buffer, batchIO[1].len, batchIO[1].offset);
        ASSERT_EQ(1, counter.blockCacheHitCount + counter.blockCacheMissCount);
    }
    {
        // decompress 1 time for 2 singleIO in same block
        BatchIO batchIO({{buffer[0], 100, 4096 * 3}, {buffer[1], 200, 4096 * 3 + 100}});
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, option));
        ASSERT_EQ(100, result[0].GetOrThrow());
        ASSERT_EQ(200, result[1].GetOrThrow());
        CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
        CheckData(oriData, batchIO[1].buffer, batchIO[1].len, batchIO[1].offset);
        ASSERT_EQ(1, counter.blockCacheHitCount + counter.blockCacheMissCount);
    }
    {
        // decompress 1 time for 2 BatchIO in same block
        {
            BatchIO batchIO({{buffer[0], 100, 4096 * 4}});
            util::BlockAccessCounter counter;
            ReadOption option;
            option.blockCounter = &counter;
            auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, option));

            ASSERT_EQ(100, result[0].GetOrThrow());
            CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
            ASSERT_EQ(1, counter.blockCacheHitCount + counter.blockCacheMissCount);
        }
        {
            BatchIO batchIO({{buffer[0], 100, 4096 * 4}});
            util::BlockAccessCounter counter;
            ReadOption option;
            option.blockCounter = &counter;
            auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, option));
            ASSERT_EQ(100, result[0].GetOrThrow());
            CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
            ASSERT_EQ(0, counter.blockCacheHitCount + counter.blockCacheMissCount);
        }
        {
            BatchIO batchIO({{buffer[0], 100, 4096 * 4 + 200}});
            util::BlockAccessCounter counter;
            ReadOption option;
            option.blockCounter = &counter;
            auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, option));
            ASSERT_EQ(100, result[0].GetOrThrow());
            CheckData(oriData, batchIO[0].buffer, batchIO[0].len, batchIO[0].offset);
            ASSERT_EQ(0, counter.blockCacheHitCount + counter.blockCacheMissCount);
        }
    }
}

void CompressFileReaderTest::TestException()
{
    using ErrorGenerator = fslib::fs::ErrorGenerator;
    fslib::fs::FileSystem::_useMock = true;
    string loadConfig = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "cache_decompress_file" : true,
                "memory_size_in_mb" : 0
            }
        }
        ]
    })";

    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    string oriData;
    PrepareFile(oriData);
    auto dir = _directory;
    string filePath = "tmp";

    auto errorGenerator = ErrorGenerator::getInstance();
    ErrorGenerator::FileSystemErrorMap errorMap;
    fslib::fs::FileSystemError ec;
    ec.ec = fslib::ErrorCode::EC_BADARGS;
    ec.offset = 0;
    ec.until = 2;
    string fullFilePath = _directory->GetPhysicalPath(filePath);
    errorMap.insert({{fullFilePath, "pread"}, ec});
    errorGenerator->setErrorMap(errorMap);
    char buffer[100][8192];
    auto fileReader = dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG));
    {
        BatchIO batchIO({{buffer[0], 1024, 0}, {buffer[1], 1024, 0}});
        auto result = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_FALSE(result[0].OK());
        ASSERT_FALSE(result[1].OK());
    }
    fslib::fs::FileSystem::_useMock = false;
}

}} // namespace indexlib::file_system
