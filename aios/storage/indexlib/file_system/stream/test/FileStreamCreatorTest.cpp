#include "indexlib/file_system/stream/FileStreamCreator.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/stream/BlockFileStream.h"
#include "indexlib/file_system/stream/CompressFileStream.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/NormalFileStream.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "unittest/unittest.h"

namespace indexlib::file_system {

class FileStreamCreatorTest : public TESTBASE
{
public:
    FileStreamCreatorTest() = default;
    ~FileStreamCreatorTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<FileReader> PrepareFileReader(std::string fileName, bool compressed);

    template <typename Type>
    void InnerTestCreatFileStream(std::string readMode, bool supportConcurrency);
    void InnerTestCreateCompressFileStream(std::string readMode, bool supportConcurrency);

    std::shared_ptr<IDirectory> GetRootDirectory(std::string readMode);

private:
    std::string _rootPath;
    std::shared_ptr<IDirectory> _rootDir;
};

void FileStreamCreatorTest::setUp() { _rootPath = GET_TEMP_DATA_PATH() + "/"; }

void FileStreamCreatorTest::tearDown() {}

std::shared_ptr<IDirectory> FileStreamCreatorTest::GetRootDirectory(std::string readMode)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    indexlib::file_system::LoadConfigList loadConfigList;
    if (!readMode.empty()) {
        loadConfigList = LoadConfigListCreator::CreateLoadConfigList(readMode);
    } else {
        auto loadStrategy =
            std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
        indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
        pattern.push_back(".*");

        indexlib::file_system::LoadConfig loadConfig;
        loadConfig.SetLoadStrategyPtr(loadStrategy);
        loadConfig.SetFilePatternString(pattern);
        loadConfig.SetLoadStrategyPtr(loadStrategy);
        loadConfig.SetName("__FILE_STREAM_TEST__");
        loadConfigList.PushFront(loadConfig);
    }
    fsOptions.loadConfigList = loadConfigList;
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    assert(rootDir != nullptr);
    _rootDir = rootDir->GetIDirectory();
    assert(_rootDir != nullptr);
    return _rootDir;
}

std::shared_ptr<FileReader> FileStreamCreatorTest::PrepareFileReader(std::string readMode, bool compressed)
{
    static size_t fileNo = 0;
    auto directory = GetRootDirectory(readMode);
    std::string fileName = "data_" + std::to_string(fileNo++);

    file_system::WriterOption option = file_system::WriterOption::Buffer();
    if (compressed) {
        option.compressorName = "lz4";
        option.compressBufferSize = 1024;
    }
    auto writer = directory->CreateFileWriter(fileName, option).GetOrThrow();
    size_t dataLen = 73 * 1024; // 73KB
    std::string oriData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        oriData[i] = (char)(i * 3 % 127);
    }
    writer->Write(oriData.data(), dataLen).GetOrThrow();
    writer->Close().GetOrThrow();
    ReaderOption readOption(FSOT_LOAD_CONFIG);
    readOption.supportCompress = compressed;
    return directory->CreateFileReader(fileName, readOption).GetOrThrow();
}

template <typename Type>
void FileStreamCreatorTest::InnerTestCreatFileStream(std::string readMode, bool supportConcurrency)
{
    auto fileReader = PrepareFileReader(readMode, false);
    auto stream = supportConcurrency ? FileStreamCreator::CreateConcurrencyFileStream(fileReader, nullptr)
                                     : FileStreamCreator::CreateFileStream(fileReader, nullptr);
    ASSERT_TRUE(stream);
    ASSERT_TRUE(std::dynamic_pointer_cast<Type>(stream));
}

void FileStreamCreatorTest::InnerTestCreateCompressFileStream(std::string readMode, bool supportConcurrency)
{
    auto fileReader = PrepareFileReader(readMode, true);
    auto stream = supportConcurrency ? FileStreamCreator::CreateConcurrencyFileStream(fileReader, nullptr)
                                     : FileStreamCreator::CreateFileStream(fileReader, nullptr);
    ASSERT_TRUE(stream);
    auto compressStream = std::dynamic_pointer_cast<CompressFileStream>(stream);
    ASSERT_TRUE(compressStream);
    ASSERT_EQ(supportConcurrency, compressStream->IsSupportConcurrency());
}

TEST_F(FileStreamCreatorTest, TestSimpleProcess)
{
    InnerTestCreatFileStream<BlockFileStream>(READ_MODE_CACHE, false);
    InnerTestCreatFileStream<BlockFileStream>(READ_MODE_CACHE, true);
    InnerTestCreatFileStream<NormalFileStream>(READ_MODE_MMAP, false);
    InnerTestCreatFileStream<NormalFileStream>(READ_MODE_MMAP, true);

    InnerTestCreateCompressFileStream(READ_MODE_CACHE, false);
    InnerTestCreateCompressFileStream(READ_MODE_CACHE, true);
    InnerTestCreateCompressFileStream(READ_MODE_MMAP, false);
    InnerTestCreateCompressFileStream(READ_MODE_MMAP, true);
}

TEST_F(FileStreamCreatorTest, TestException)
{
    std::string fileName = "buffer";
    auto directory = GetRootDirectory("");
    auto writer = directory->CreateFileWriter(fileName, file_system::WriterOption::Buffer()).GetOrThrow();
    size_t dataLen = 73 * 1024; // 73KB
    std::string oriData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        oriData[i] = (char)(i * 3 % 127);
    }
    writer->Write(oriData.data(), dataLen).GetOrThrow();
    writer->Close().GetOrThrow();
    auto fileReader = directory->CreateFileReader(fileName, file_system::FSOT_BUFFERED).GetOrThrow();
    ASSERT_TRUE(fileReader);

    ASSERT_TRUE(FileStreamCreator::CreateFileStream(fileReader, nullptr));
    ASSERT_FALSE(FileStreamCreator::CreateConcurrencyFileStream(fileReader, nullptr));
}

} // namespace indexlib::file_system
