#include "indexlib/file_system/stream/BlockFileStream.h"

#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/cache/BlockCacheOption.h"
#include "unittest/unittest.h"

namespace indexlib::file_system {

class BlockFileStreamTest : public TESTBASE
{
public:
    BlockFileStreamTest() = default;
    ~BlockFileStreamTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void GenerateFile(const std::string& fileName, size_t blockSize);
};

void BlockFileStreamTest::setUp() {}

void BlockFileStreamTest::tearDown() {}

void BlockFileStreamTest::GenerateFile(const std::string& fileName, size_t blockSize)
{
    constexpr size_t size = 1024;
    std::string content(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        content[i] = i;
    }
    if (FslibWrapper::IsExist(fileName).GetOrThrow()) {
        ASSERT_EQ(file_system::FSEC_OK,
                  FslibWrapper::DeleteFile(fileName, file_system::DeleteOption::NoFence(false)).Code());
    }
    auto file = FslibWrapper::OpenFile(fileName, fslib::WRITE).GetOrThrow();
    assert(file != NULL);
    file->WriteE(content.data(), content.size());
    file->CloseE();
}

TEST_F(BlockFileStreamTest, TestSimpleProcess)
{
    constexpr size_t blockSize = 16;
    constexpr size_t ioBatchSize = 4;
    std::string fileName = GET_TEMP_DATA_PATH() + "/block_file";
    GenerateFile(fileName, blockSize);
    std::shared_ptr<util::BlockCache> cache(
        util::BlockCacheCreator::Create(util::BlockCacheOption::LRU(100 * blockSize, blockSize, ioBatchSize)));
    auto blockFileNode = std::make_shared<file_system::BlockFileNode>(cache.get(), false, false, false, "");
    ASSERT_EQ(file_system::FSEC_OK, blockFileNode->Open("LOGICAL_PATH", fileName, file_system::FSOT_CACHE, -1));
    auto fileReader = std::make_shared<file_system::NormalFileReader>(blockFileNode);
    auto stream = std::make_shared<BlockFileStream>(fileReader, false);

    // expected stream holds fileReader life-cycle
    fileReader.reset();
    ASSERT_EQ(1024, stream->GetStreamLength());
    char buffer[64];
    ASSERT_EQ(8, stream->Read(buffer, 8, 0, ReadOption()).GetOrThrow());
    ASSERT_EQ(8, stream->Read(buffer, 8, 8, ReadOption()).GetOrThrow());
    // expeceted only access block cache one time
    ASSERT_EQ(1, cache->GetTotalHitCount() + cache->GetTotalMissCount());

    // expected CreateSession without sharing status
    auto sessionStream = stream->CreateSessionStream(nullptr);
    ASSERT_TRUE(sessionStream);
    ASSERT_EQ(8, sessionStream->Read(buffer, 8, 0, ReadOption()).GetOrThrow());
    ASSERT_EQ(8, sessionStream->Read(buffer, 8, 8, ReadOption()).GetOrThrow());
    ASSERT_EQ(2, cache->GetTotalHitCount() + cache->GetTotalMissCount());
}

TEST_F(BlockFileStreamTest, TestBatchRead)
{
    constexpr size_t blockSize = 16;
    constexpr size_t ioBatchSize = 4;
    std::string fileName = GET_TEMP_DATA_PATH() + "/block_file";
    GenerateFile(fileName, blockSize);
    std::shared_ptr<util::BlockCache> cache(
        util::BlockCacheCreator::Create(util::BlockCacheOption::LRU(100 * blockSize, blockSize, ioBatchSize)));
    auto blockFileNode = std::make_shared<file_system::BlockFileNode>(cache.get(), false, false, false, "");
    ASSERT_EQ(file_system::FSEC_OK, blockFileNode->Open("LOGICAL_PATH", fileName, file_system::FSOT_CACHE, -1));
    auto fileReader = std::make_shared<file_system::NormalFileReader>(blockFileNode);
    auto stream = std::make_shared<BlockFileStream>(fileReader, false);
    char buffer[10][4096];
    {
        // normal case
        file_system::BatchIO batchIO({{buffer[0], 30, 0}, {buffer[1], 50, 3}});
        auto result = future_lite::coro::syncAwait(stream->BatchRead(batchIO, file_system::ReadOption()));
        ASSERT_EQ(batchIO.size(), result.size());
        for (size_t i = 0; i < batchIO.size(); ++i) {
            ASSERT_EQ(batchIO[i].len, result[i].GetOrThrow());
        }
    }

    {
        // unsorted offset
        file_system::BatchIO batchIO({{buffer[0], 30, 10}, {buffer[1], 50, 3}});
        auto result = future_lite::coro::syncAwait(stream->BatchRead(batchIO, file_system::ReadOption()));
        ASSERT_EQ(batchIO.size(), result.size());
        for (size_t i = 0; i < batchIO.size(); ++i) {
            ASSERT_EQ(batchIO[i].len, result[i].GetOrThrow());
        }
    }
}

TEST_F(BlockFileStreamTest, TestConcurrency)
{
    constexpr size_t blockSize = 16;
    constexpr size_t ioBatchSize = 4;
    std::string fileName = GET_TEMP_DATA_PATH() + "/block_file";
    GenerateFile(fileName, blockSize);
    std::shared_ptr<util::BlockCache> cache(
        util::BlockCacheCreator::Create(util::BlockCacheOption::LRU(100 * blockSize, blockSize, ioBatchSize)));
    auto blockFileNode = std::make_shared<file_system::BlockFileNode>(cache.get(), false, false, false, "");
    ASSERT_EQ(file_system::FSEC_OK, blockFileNode->Open("LOGICAL_PATH", fileName, file_system::FSOT_CACHE, -1));
    auto fileReader = std::make_shared<file_system::NormalFileReader>(blockFileNode);
    auto stream = FileStreamCreator::CreateConcurrencyFileStream(fileReader, nullptr);

    // test concurrency, invoke read concurrently from one file reader reader
    std::vector<std::shared_ptr<autil::Thread>> threads;
    for (size_t i = 0; i < 10; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread([&]() {
            char buffer[64] = {'\0'};
            auto fileStream = stream->CreateSessionStream(nullptr);
            for (size_t j = 0; j < 10000; ++j) {
                ASSERT_EQ(8, fileStream->Read(buffer, 8, 'a', ReadOption()).GetOrThrow());
                ASSERT_EQ(0, strncmp(buffer, "abcdefgh", 8));
                ASSERT_EQ(8, fileStream->Read(buffer, 8, 'A', ReadOption()).GetOrThrow());
                ASSERT_EQ(0, strncmp(buffer, "ABCDEFGH", 8));
                ASSERT_EQ(8, fileStream->Read(buffer, 8, 'A' + 512, ReadOption()).GetOrThrow());
                ASSERT_EQ(0, strncmp(buffer, "ABCDEFGH", 8));
                ASSERT_EQ(8, fileStream->Read(buffer, 8, 'a' + 512, ReadOption()).GetOrThrow());
                ASSERT_EQ(0, strncmp(buffer, "abcdefgh", 8));
            }
        });
        threads.emplace_back(thread);
    }
    for (auto& thread : threads) {
        thread->join();
    }
}

TEST_F(BlockFileStreamTest, TestException)
{
    constexpr size_t blockSize = 16;
    constexpr size_t ioBatchSize = 4;
    std::string fileName = GET_TEMP_DATA_PATH() + "/block_file";
    GenerateFile(fileName, blockSize);
    std::shared_ptr<util::BlockCache> cache(
        util::BlockCacheCreator::Create(util::BlockCacheOption::LRU(100 * blockSize, blockSize, ioBatchSize)));
    auto blockFileNode = std::make_shared<file_system::BlockFileNode>(cache.get(), false, false, false, "");
    ASSERT_EQ(file_system::FSEC_OK, blockFileNode->Open("LOGICAL_PATH", fileName, file_system::FSOT_CACHE, -1));
    auto fileReader = std::make_shared<file_system::NormalFileReader>(blockFileNode);
    auto stream = std::make_shared<BlockFileStream>(fileReader, false);

    fileReader.reset();
    size_t dataLength = stream->GetStreamLength();
    char buffer[64];
    ASSERT_FALSE(stream->Read(buffer, 1, dataLength, ReadOption()).OK());
    ASSERT_EQ(0, stream->Read(buffer, 0, dataLength, ReadOption()).GetOrThrow());
    ASSERT_EQ(0, cache->GetTotalHitCount() + cache->GetTotalMissCount());
}

} // namespace indexlib::file_system
