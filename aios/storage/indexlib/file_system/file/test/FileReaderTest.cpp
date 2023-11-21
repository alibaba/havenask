#include "indexlib/file_system/file/FileReader.h"

#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class DummyFileReader : public FileReader
{
public:
    DummyFileReader(size_t length) noexcept : _length(length), _offset(0) {}
    FSResult<void> Open() noexcept override { return FSEC_OK; }
    FSResult<void> Close() noexcept override { return FSEC_OK; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override
    {
        if (offset + length > _length) {
            AUTIL_LOG(ERROR, "outof range [%lu] [%lu] [%lu]", offset, length, _length);
            return {FSEC_BADARGS, 0};
        }
        _offset = offset + length;
        return {FSEC_OK, length};
    }
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override
    {
        if (_offset + length > _length) {
            AUTIL_LOG(ERROR, "outof range ");
            return {FSEC_BADARGS, 0};
        }
        _offset = _offset + length;
        return {FSEC_OK, length};
    }
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset,
                                             ReadOption option = ReadOption()) noexcept override
    {
        return nullptr;
    }
    void* GetBaseAddress() const noexcept override { return nullptr; }
    size_t GetLength() const noexcept override { return _length; }

    // logic length match with offset, which is equal to uncompress file length for compress file
    size_t GetLogicLength() const noexcept override { return GetLength(); }
    const std::string& GetLogicalPath() const noexcept override { return _path; }
    const std::string& GetPhysicalPath() const noexcept override { return _path; }
    FSOpenType GetOpenType() const noexcept override { return {}; }
    FSFileType GetType() const noexcept override { return {}; }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return {}; }

private:
    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                            ReadOption option) noexcept override
    {
        assert(std::is_sorted(batchIO.begin(), batchIO.end()));
        std::vector<FSResult<size_t>> result(batchIO.size());
        for (size_t i = 0; i < batchIO.size(); ++i) {
            auto& singleIO = batchIO[i];
            result[i] = Read(singleIO.buffer, singleIO.len, singleIO.offset, option);
        }
        co_return result;
    }

private:
    size_t _length;
    size_t _offset;
    string _path;
};

class FileReaderTest : public INDEXLIB_TESTBASE
{
public:
    FileReaderTest();
    ~FileReaderTest();

    DECLARE_CLASS_NAME(FileReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBartchReadNonIncreasingOrder();
    void TestReadOutOfRange();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileReaderTest, TestBartchReadNonIncreasingOrder);
INDEXLIB_UNIT_TEST_CASE(FileReaderTest, TestReadOutOfRange);
AUTIL_LOG_SETUP(indexlib.file_system, FileReaderTest);

FileReaderTest::FileReaderTest() {}

FileReaderTest::~FileReaderTest() {}

void FileReaderTest::CaseSetUp() {}

void FileReaderTest::CaseTearDown() {}

void FileReaderTest::TestBartchReadNonIncreasingOrder()
{
    std::shared_ptr<FileReader> fileReader(new DummyFileReader(40960));
    {
        BatchIO batchIO;
        char buffer[4][4096];
        batchIO.emplace_back(buffer[0], 4096, 4096);
        batchIO.emplace_back(buffer[1], 4096, 0);

        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(4096, readResult[0].GetOrThrow());
        ASSERT_EQ(4096, readResult[1].GetOrThrow());
    }
    {
        BatchIO batchIO;
        char buffer[4][4096];
        batchIO.emplace_back(buffer[0], 1, 4096);
        batchIO.emplace_back(buffer[1], 4096, 0);

        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_EQ(1, readResult[0].GetOrThrow());
        ASSERT_EQ(4096, readResult[1].GetOrThrow());
    }
    {
        BatchIO batchIO;
        char buffer[30][4096];
        vector<size_t> expectedResult;
        for (size_t i = 0; i < 30; ++i) {
            size_t offset = random() % (4096 * 9);
            size_t len = random() % 40960;
            len = min(len, 40960 - offset);
            batchIO.emplace_back(buffer[i], len, offset);
            expectedResult.push_back(len);
        }
        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        for (size_t i = 0; i < expectedResult.size(); ++i) {
            ASSERT_EQ(expectedResult[i], readResult[i].GetOrThrow());
        }
    }
}

void FileReaderTest::TestReadOutOfRange()
{
    std::shared_ptr<FileReader> fileReader(new DummyFileReader(4096));
    {
        BatchIO batchIO;
        char buffer[4][4096];
        batchIO.emplace_back(buffer[0], 4096, 40960000);
        batchIO.emplace_back(buffer[1], 4096, 0);

        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_FALSE(readResult[0].OK());
        ASSERT_FALSE(readResult[1].OK());
    }
    {
        BatchIO batchIO;
        char buffer[4][4097];
        batchIO.emplace_back(buffer[0], 4097, 0);

        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_FALSE(readResult[0].OK());
    }
    {
        BatchIO batchIO;
        char buffer[4][4096];
        batchIO.emplace_back(buffer[0], 4096, 0);

        auto readResult = future_lite::coro::syncAwait(fileReader->BatchRead(batchIO, ReadOption()));
        ASSERT_TRUE(readResult[0].OK());
        ASSERT_EQ(4096, readResult[0].GetOrThrow());
    }
}

}} // namespace indexlib::file_system
