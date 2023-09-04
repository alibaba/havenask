#include "indexlib/file_system/stream/CompressFileStream.h"

#include "autil/Thread.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "unittest/unittest.h"

namespace indexlib::file_system {

class CompressFileStreamTest : public TESTBASE
{
public:
    CompressFileStreamTest() = default;
    ~CompressFileStreamTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void InnerTest(std::string readMode);
    void CheckRead(const std::shared_ptr<FileStream>& fileStream, std::string oriData);

private:
    std::shared_ptr<IDirectory> GetRootDirectory(std::string readMode);

private:
    std::string _rootPath;
};

void CompressFileStreamTest::setUp() { _rootPath = GET_TEMP_DATA_PATH() + "/"; }

void CompressFileStreamTest::tearDown() {}

std::shared_ptr<IDirectory> CompressFileStreamTest::GetRootDirectory(std::string readMode)
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
    auto rootIDir = rootDir->GetIDirectory();
    assert(rootIDir != nullptr);
    return rootIDir;
}

void CompressFileStreamTest::CheckRead(const std::shared_ptr<FileStream>& fileStream, std::string oriData)
{
    // check read success
    ASSERT_EQ(oriData.length(), fileStream->GetStreamLength());
    std::vector<char> buffer(oriData.length());
    size_t readed = fileStream->Read(buffer.data(), buffer.size(), 0, ReadOption()).GetOrThrow();
    ASSERT_EQ(buffer.size(), readed);
    ASSERT_TRUE(oriData.compare(0, readed, buffer.data(), readed) == 0);
}

void CompressFileStreamTest::InnerTest(std::string readMode)
{
    std::string filePath = "tmp";
    auto dir = GetRootDirectory(readMode);
    auto compressWriter = std::dynamic_pointer_cast<CompressFileWriter>(
        dir->CreateFileWriter(filePath, file_system::WriterOption::Compress("lz4", 1024)).GetOrThrow());
    auto fileWriter = compressWriter->GetDataFileWriter();

    size_t dataLen = 73 * 1024; // 73KB
    std::string oriData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        // oriData[i] = 'A' + i % 26;
        oriData[i] = (char)(i * 3 % 127);
    }
    compressWriter->Write(oriData.data(), oriData.size()).GetOrThrow();
    ASSERT_EQ(fileWriter->GetLogicalPath(), compressWriter->GetLogicalPath());
    ASSERT_EQ(fileWriter->GetLength(), compressWriter->GetLength());
    ASSERT_TRUE(compressWriter->GetLength() < dataLen);
    compressWriter->Close().GetOrThrow();

    {
        auto compressReader = std::dynamic_pointer_cast<CompressFileReader>(
            dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG)).GetOrThrow());
        auto fileStream = std::make_shared<CompressFileStream>(compressReader, false, nullptr);
        compressReader.reset();
        CheckRead(fileStream, oriData);
        auto sessionStream = fileStream->CreateSessionStream(nullptr);
        ASSERT_TRUE(sessionStream);
        CheckRead(sessionStream, oriData);
    }

    {
        auto compressReader = std::dynamic_pointer_cast<CompressFileReader>(
            dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG)).GetOrThrow());
        auto fileStream = std::make_shared<CompressFileStream>(compressReader, true, nullptr);
        compressReader.reset();
        // test concurrency, invoke read concurrently from one reader
        std::vector<std::shared_ptr<autil::Thread>> threads;
        for (size_t i = 0; i < 5; ++i) {
            autil::ThreadPtr thread = autil::Thread::createThread([&]() {
                for (size_t j = 0; j < 10000; ++j) {
                    CheckRead(fileStream, oriData);
                }
            });
            threads.emplace_back(thread);
        }
        for (auto& thread : threads) {
            thread->join();
        }
    }
}

TEST_F(CompressFileStreamTest, TestMemory) { InnerTest(READ_MODE_MMAP); }

TEST_F(CompressFileStreamTest, TestBlockCache) { InnerTest(READ_MODE_CACHE); }

TEST_F(CompressFileStreamTest, TestConcurreny)
{
    std::string filePath = "tmp";
    auto dir = GetRootDirectory("");
    auto compressWriter = dir->CreateFileWriter(filePath, WriterOption::Compress("lz4", 1024)).GetOrThrow();
    size_t dataLen = 73 * 1024; // 73KB
    std::string oriData(dataLen, '\0');
    for (size_t i = 0; i < dataLen; ++i) {
        // oriData[i] = 'A' + i % 26;
        oriData[i] = (char)(i * 3 % 127);
    }
    compressWriter->Write(oriData.data(), oriData.size()).GetOrThrow();
    compressWriter->Close().GetOrThrow();

    auto compressReader = std::dynamic_pointer_cast<CompressFileReader>(
        dir->CreateFileReader(filePath, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG)).GetOrThrow());
    auto originStream = std::make_shared<CompressFileStream>(compressReader, false, nullptr);

    // expected all stream from one file reader can read in same time
    // we expect session streams share no status in user view
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < 10; ++i) {
        auto thread = autil::Thread::createThread([&]() {
            auto fileStream = originStream->CreateSessionStream(nullptr);
            for (size_t j = 0; j < 10000; ++j) {
                CheckRead(fileStream, oriData);
            }
            fileStream.reset(new CompressFileStream(compressReader, false, nullptr));
            for (size_t j = 0; j < 10000; ++j) {
                CheckRead(fileStream, oriData);
            }
        });
        threads.emplace_back(thread);
    }
    for (auto& thread : threads) {
        thread->join();
    }
}

} // namespace indexlib::file_system
