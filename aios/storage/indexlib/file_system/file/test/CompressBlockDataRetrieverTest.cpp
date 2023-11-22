#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/DecompressCachedBlockDataRetriever.h"
#include "indexlib/file_system/file/NormalCompressBlockDataRetriever.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/cache/MemoryBlockCache.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class CompressBlockDataRetrieverTest : public INDEXLIB_TESTBASE
{
public:
    CompressBlockDataRetrieverTest();
    ~CompressBlockDataRetrieverTest();

    DECLARE_CLASS_NAME(CompressBlockDataRetrieverTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalCompressBlockDataRetriever();
    void TestDecompressCachedBlockDataRetriever();

private:
    void PrepareFile(const string& fileContent);
    void InnerTest(bool cacheDecompress);
    std::string GetLoadConfig(bool cacheDecompress);

    std::shared_ptr<FileWriter> CreateFileWriter(const std::shared_ptr<Directory>& dir, const std::string& path,
                                                 size_t blockSize)
    {
        return dir->CreateFileWriter(path, WriterOption::Compress("lz4", blockSize));
    }

private:
    size_t _blockSize;
    std::string _rootDir;
    std::string _fileName;
    util::BlockCachePtr _blockCache;
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, CompressBlockDataRetrieverTest);

INDEXLIB_UNIT_TEST_CASE(CompressBlockDataRetrieverTest, TestNormalCompressBlockDataRetriever);
INDEXLIB_UNIT_TEST_CASE(CompressBlockDataRetrieverTest, TestDecompressCachedBlockDataRetriever);
//////////////////////////////////////////////////////////////////////

CompressBlockDataRetrieverTest::CompressBlockDataRetrieverTest() {}

CompressBlockDataRetrieverTest::~CompressBlockDataRetrieverTest() {}

void CompressBlockDataRetrieverTest::PrepareFile(const string& fileContent)
{
    auto dir = _directory;
    string filePath = "tmp.retriever";
    CompressFileWriterPtr compressWriter =
        std::dynamic_pointer_cast<CompressFileWriter>(CreateFileWriter(dir, filePath, 4));
    std::shared_ptr<FileWriter> fileWriter = compressWriter->GetDataFileWriter();

    ASSERT_EQ(FSEC_OK, compressWriter->Write(fileContent.data(), fileContent.size()).Code());
    ASSERT_EQ(fileWriter->GetLogicalPath(), compressWriter->GetLogicalPath());
    ASSERT_EQ(fileWriter->GetLength(), compressWriter->GetLength());
    ASSERT_EQ(FSEC_OK, compressWriter->Close());
}

string CompressBlockDataRetrieverTest::GetLoadConfig(bool cacheDecompress)
{
    string jsonStr;
    if (cacheDecompress) {
        jsonStr = R"(
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
    } else {
        jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache"
        }
        ]
    })";
    }
    return jsonStr;
}

void CompressBlockDataRetrieverTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _fileName = _rootDir + "block_file";
    _blockCache.reset(new util::MemoryBlockCache);
    _blockSize = 4;
    _blockCache->Init(BlockCacheOption::LRU(4096, _blockSize, 4));
}

void CompressBlockDataRetrieverTest::CaseTearDown() {}

void CompressBlockDataRetrieverTest::InnerTest(bool cacheDecompress)
{
    string loadConfig = GetLoadConfig(cacheDecompress);
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);

    string oriData = "aaaabbbbccccddddeeeefff";
    PrepareFile(oriData);
    auto dir = _directory;
    string filePath = "tmp.retriever";

    auto fileReader = dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG));
    auto compressFileReader = dynamic_pointer_cast<CompressFileReader>(fileReader);
    ASSERT_TRUE(compressFileReader);
    auto dataFileReader = compressFileReader->GetDataFileReader();
    ReadOption option;
    std::shared_ptr<CompressFileInfo> compressFileInfo = compressFileReader->GetCompressInfo();
    std::shared_ptr<CompressFileAddressMapper> mapper = compressFileReader->_compressAddrMapper;
    autil::mem_pool::Pool pool;

    CompressBlockDataRetriever* dataRetriever = nullptr;
    if (cacheDecompress) {
        dataRetriever =
            new DecompressCachedBlockDataRetriever(option, compressFileInfo, mapper.get(), dataFileReader.get(), &pool);
    } else {
        dataRetriever =
            new NormalCompressBlockDataRetriever(option, compressFileInfo, mapper.get(), dataFileReader.get(), &pool);
    }
    auto CheckPrefetch = [compressFileReader](CompressBlockDataRetriever& retriever, size_t offset, size_t length,
                                              string content) {
        ASSERT_EQ(length, content.length());
        ASSERT_EQ(ErrorCode::FSEC_OK, future_lite::coro::syncAwait(retriever.Prefetch(offset, length)));
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        string buffer(content.size(), '-');
        ASSERT_EQ(content.size(), compressFileReader->Read(buffer.data(), length, offset, option).GetOrThrow());
        ASSERT_EQ(content, buffer);
        ASSERT_EQ(0, counter.blockCacheMissCount);
        ASSERT_TRUE(counter.blockCacheHitCount > 0);
    };

    CheckPrefetch(*dataRetriever, 0, 4, "aaaa");
    CheckPrefetch(*dataRetriever, 20, 3, "fff");
    CheckPrefetch(*dataRetriever, 1, 6, "aaabbb");

    delete dataRetriever;
}

void CompressBlockDataRetrieverTest::TestNormalCompressBlockDataRetriever() { InnerTest(false); }

void CompressBlockDataRetrieverTest::TestDecompressCachedBlockDataRetriever() { InnerTest(true); }

}} // namespace indexlib::file_system
