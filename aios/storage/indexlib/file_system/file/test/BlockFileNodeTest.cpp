#include "indexlib/file_system/file/BlockFileNode.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string.h>
#include <sys/types.h>
#include <type_traits>
#include <utility>
#include <vector>

#include "fslib/common/common_type.h"
#include "fslib/fs/ErrorGenerator.h"
#include "fslib/fs/FileSystem.h"
#include "future_lite/Common.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Promise.h"
#include "future_lite/Unit.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "future_lite/executors/SimpleIOExecutor.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibCommonFileWrapper.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/cache/BlockHandle.h"
#include "indexlib/util/testutil/unittest.h"

namespace future_lite {
class Executor;
} // namespace future_lite

using namespace std;
using namespace future_lite;
using namespace future_lite::executors;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BlockFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    BlockFileNodeTest();
    ~BlockFileNodeTest();

    DECLARE_CLASS_NAME(BlockFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForReadWithBuffer();
    void TestCaseForReadWithBufferCoro();
    void TestCaseForReadWithDirectIO();
    void TestCaseForReadWithDirectIOCoro();
    void TestCaseForReadWithByteSliceList();
    void TestCaseForReadFileOutOfRangeWithBuffer();
    /*
    void TestCaseForReadFileOutOfRangeWithBufferCoro();
    */
    void TestCaseForReadFileOutOfRangeWithByteSliceList();
    void TestCaseForReadEmptyFileWithCache();
    void TestCaseForGetBlocks();
    void TestCaseForGetBaseAddressException();
    void TestCaseForWriteException();
    void TestCaseForReadWithByteSliceListException();
    void TestCaseForPrefetch();
    void TestCaseForNonContinuousMissBlock();
    void TestCaseForReadUInt32Async();
    void TestMaxStackSize();
    void TestCaseForBatchReadMergeBlock();
    void TestCaseForMultiThreadBatchRead();
    void TestCaseForBatchReadWithIOError();

private:
    void GenerateFile(const std::string& fileName, size_t size, size_t blockSize);
    void GenerateFile(const std::string& fileName, const std::string& content, size_t blockSize);
    std::shared_ptr<BlockFileNode> CreateFileNode(bool useDirectIO = false);

private:
    std::string _rootDir;
    std::string _fileName;
    uint64_t _blockSize;
    size_t _iOBatchSize;
    util::BlockCachePtr _blockCache;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileNodeTest);

INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithBuffer);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithBufferCoro);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithDirectIO);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithDirectIOCoro);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithByteSliceList);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadFileOutOfRangeWithBuffer);
/*
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadFileOutOfRangeWithBufferCoro);
*/
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadFileOutOfRangeWithByteSliceList);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadEmptyFileWithCache);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForGetBaseAddressException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForWriteException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithByteSliceListException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForPrefetch);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForNonContinuousMissBlock);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadUInt32Async);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestMaxStackSize);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForBatchReadMergeBlock);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForMultiThreadBatchRead);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForBatchReadWithIOError);
//////////////////////////////////////////////////////////////////////

BlockFileNodeTest::BlockFileNodeTest() {}

BlockFileNodeTest::~BlockFileNodeTest() {}

void BlockFileNodeTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_NON_RAMDISK_PATH());
    _fileName = _rootDir + "block_file";
    _blockSize = 4;
    _iOBatchSize = 4;
}

void BlockFileNodeTest::CaseTearDown() {}

#ifndef CHECK_FILE_NODE_WITH_BUFFER
#define CHECK_FILE_NODE_WITH_BUFFER(expectData, offset, length)                                                        \
    {                                                                                                                  \
        const uint32_t fileSize = 9;                                                                                   \
        GenerateFile(_fileName, fileSize, _blockSize);                                                                 \
        std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();                                               \
        ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));                            \
        char buffer[length];                                                                                           \
        ASSERT_EQ(FSEC_OK, blockFileNode->Read(buffer, length, offset, ReadOption()).Code());                          \
        const string curData(buffer, length);                                                                          \
        ASSERT_EQ(expectData, curData.substr(0, length));                                                              \
    }
#endif

void BlockFileNodeTest::TestCaseForReadWithBuffer()
{
    CHECK_FILE_NODE_WITH_BUFFER(string(3, 'a'), 0, 3);
    CHECK_FILE_NODE_WITH_BUFFER(string(4, 'a'), 0, 4);
    CHECK_FILE_NODE_WITH_BUFFER(string(4, 'a') + string(1, 'b'), 0, 5);

    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a'), 2, 1);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a'), 2, 2);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a') + string(2, 'b'), 2, 4);
    CHECK_FILE_NODE_WITH_BUFFER(string(2, 'a') + string(4, 'b'), 2, 6);

    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a'), 3, 1);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(1, 'b'), 3, 2);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(4, 'b'), 3, 5);
    CHECK_FILE_NODE_WITH_BUFFER(string(1, 'a') + string(4, 'b') + string(1, 'c'), 3, 6);
}
#undef CHECK_FILE_NODE_WITH_BUFFER

#ifndef CHECK_FILE_NODE_WITH_BUFFER_CORO
#define CHECK_FILE_NODE_WITH_BUFFER_CORO(expectData, offset, length)                                                   \
    {                                                                                                                  \
        const uint32_t fileSize = 9;                                                                                   \
        GenerateFile(_fileName, fileSize, _blockSize);                                                                 \
        std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode(false);                                          \
        ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));                            \
        char buffer[length];                                                                                           \
        auto executor = ExecutorCreator::Create(                                                                       \
            /*type*/ "async_io", ExecutorCreator::Parameters().SetThreadNum(10).SetExecutorName("suez_write"));        \
        ASSERT_EQ(FSEC_OK, future_lite::interface::syncAwait(                                                          \
                               blockFileNode->ReadAsyncCoro(buffer, length, offset, ReadOption()))                     \
                               .Code());                                                                               \
        const string curData(buffer, length);                                                                          \
        ASSERT_EQ(expectData, curData.substr(0, length));                                                              \
    }
#endif

void BlockFileNodeTest::TestCaseForReadWithBufferCoro()
{
    _blockSize = 4;
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(3, 'a'), 0, 3);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(4, 'a'), 0, 4);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(4, 'a') + string(1, 'b'), 0, 5);

    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(1, 'a'), 2, 1);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(2, 'a'), 2, 2);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(2, 'a') + string(2, 'b'), 2, 4);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(2, 'a') + string(4, 'b'), 2, 6);

    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(1, 'a'), 3, 1);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(1, 'a') + string(1, 'b'), 3, 2);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(1, 'a') + string(4, 'b'), 3, 5);
    CHECK_FILE_NODE_WITH_BUFFER_CORO(string(1, 'a') + string(4, 'b') + string(1, 'c'), 3, 6);
}
#undef CHECK_FILE_NODE_WITH_BUFFER_CORO

#ifndef CHECK_FILE_NODE_WITH_DIRECT_IO
#define CHECK_FILE_NODE_WITH_DIRECT_IO(expectData, offset, length)                                                     \
    {                                                                                                                  \
        char buffer[length];                                                                                           \
        auto executor = new SimpleExecutor(4);                                                                         \
        auto checkReadFunc = [&]() -> FL_LAZY(Unit) {                                                                  \
            const uint32_t fileSize = 10 * 1024 + 1;                                                                   \
            _blockSize = 4 * 1024;                                                                                     \
            GenerateFile(_fileName, fileSize, _blockSize);                                                             \
            std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode(true);                                       \
            EXPECT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));                        \
            blockFileNode->Read(buffer, length, offset, ReadOption()).GetOrThrow();                                    \
            FL_CORETURN future_lite::Unit {};                                                                          \
        };                                                                                                             \
        future_lite::interface::syncAwait(checkReadFunc(), executor);                                                  \
        const string curData(buffer, length);                                                                          \
        ASSERT_EQ(expectData, curData.substr(0, length));                                                              \
        delete executor;                                                                                               \
    }
#endif

void BlockFileNodeTest::TestCaseForReadWithDirectIO()
{
    CHECK_FILE_NODE_WITH_DIRECT_IO(string("aaa"), 0, 3);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string(4096, 'a'), 0, 4096);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string(4095, 'a') + "bbb", 1, 4098);
    CHECK_FILE_NODE_WITH_DIRECT_IO(string("bbb"), 5000, 3);
    CHECK_FILE_NODE_WITH_DIRECT_IO("a" + string(4096, 'b') + "ccc", 4095, 4100);
}
#undef CHECK_FILE_NODE_WITH_DIRECT_IO

#ifndef CHECK_FILE_NODE_WITH_DIRECT_IO_CORO
#define CHECK_FILE_NODE_WITH_DIRECT_IO_CORO(expectData, offset, length)                                                \
    {                                                                                                                  \
        auto executor = new SimpleExecutor(4);                                                                         \
        const uint32_t fileSize = 10 * 1024 + 1;                                                                       \
        _blockSize = 4 * 1024;                                                                                         \
        GenerateFile(_fileName, fileSize, _blockSize);                                                                 \
        std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode(true);                                           \
        ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));                            \
        char buffer[length];                                                                                           \
        ASSERT_EQ(FSEC_OK, future_lite::interface::syncAwait(                                                          \
                               blockFileNode->ReadAsyncCoro(buffer, length, offset, ReadOption()), executor)           \
                               .Code());                                                                               \
        const string curData(buffer, length);                                                                          \
        ASSERT_EQ(expectData, curData.substr(0, length));                                                              \
        delete executor;                                                                                               \
    }
#endif

void BlockFileNodeTest::TestCaseForReadWithDirectIOCoro()
{
    CHECK_FILE_NODE_WITH_DIRECT_IO_CORO(string("aaa"), 0, 3);
    CHECK_FILE_NODE_WITH_DIRECT_IO_CORO(string(4096, 'a'), 0, 4096);
    CHECK_FILE_NODE_WITH_DIRECT_IO_CORO(string(4095, 'a') + "bbb", 1, 4098);
    CHECK_FILE_NODE_WITH_DIRECT_IO_CORO(string("bbb"), 5000, 3);
    CHECK_FILE_NODE_WITH_DIRECT_IO_CORO("a" + string(4096, 'b') + "ccc", 4095, 4100);
}
#undef CHECK_FILE_NODE_WITH_DIRECT_IO_CORO

void BlockFileNodeTest::TestCaseForReadWithByteSliceList()
{
    auto CheckFileNodeWithByteSliceReader = [&](const string& expectData, uint32_t offset, uint32_t length) {
        const uint32_t fileSize = 9;
        GenerateFile(_fileName, fileSize, _blockSize);
        std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
        ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
        ByteSliceList* byteSliceList = blockFileNode->ReadToByteSliceList(length, offset, ReadOption());
        ByteSliceReader reader;
        reader.Open(byteSliceList).GetOrThrow();
        uint32_t dataLen = expectData.size();
        vector<char> dataBuffer;
        dataBuffer.resize(dataLen);
        char* data = (char*)dataBuffer.data();
        ASSERT_EQ(dataLen, reader.Read(data, dataLen).GetOrThrow());
        ASSERT_EQ(expectData, string(data, dataLen));
        delete byteSliceList;
    };
    CheckFileNodeWithByteSliceReader(string(3, 'a'), 0, 3);
    CheckFileNodeWithByteSliceReader(string(3, 'a'), 0, 3);
    CheckFileNodeWithByteSliceReader(string(4, 'a'), 0, 4);
    CheckFileNodeWithByteSliceReader(string(4, 'a') + string(1, 'b'), 0, 5);

    CheckFileNodeWithByteSliceReader(string(1, 'a'), 2, 1);
    CheckFileNodeWithByteSliceReader(string(2, 'a'), 2, 2);
    CheckFileNodeWithByteSliceReader(string(2, 'a') + string(2, 'b'), 2, 4);
    CheckFileNodeWithByteSliceReader(string(2, 'a') + string(4, 'b'), 2, 6);

    CheckFileNodeWithByteSliceReader(string(1, 'a'), 3, 1);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(1, 'b'), 3, 2);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(4, 'b'), 3, 5);
    CheckFileNodeWithByteSliceReader(string(1, 'a') + string(4, 'b') + string(1, 'c'), 3, 6);
}

void BlockFileNodeTest::TestCaseForReadFileOutOfRangeWithBuffer()
{
    const uint32_t fileSize = 9;
    const uint32_t offset = 0;
    const uint32_t len = 10;
    GenerateFile(_fileName, fileSize, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    char buffer[len];
    ASSERT_EQ(FSEC_BADARGS, blockFileNode->Read(buffer, len, offset, ReadOption()).Code());
}

// TODO(xinfei.sxf) test exception result in crash
/*
void BlockFileNodeTest::TestCaseForReadFileOutOfRangeWithBufferCoro()
{
    const uint32_t fileSize = 9;
    const uint32_t offset = 0;
    const uint32_t len = 10;
    GenerateFile(_fileName, fileSize, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    blockFileNode->Open(_fileName, _fileName,  FSOT_CACHE);
    auto executor = new SimpleExecutor(4);                               \
    char buffer[len];
    Try<size_t> try1;
    std::exception_ptr eptr;
    try1.setException(eptr);

    blockFileNode->ReadAsyncCoro(buffer, len, offset, ReadOption()).
        via(executor).start([&](Try<size_t> result) {
            eptr = result.getException();
    });
    while (!eptr) {
        usleep(1000);
    }
    // ASSERT_EQ();
    delete executor;
}
*/

void BlockFileNodeTest::TestCaseForReadFileOutOfRangeWithByteSliceList()
{
    const uint32_t offset = 0;
    const uint32_t len = 10;
    const uint32_t fileSize = 9;
    GenerateFile(_fileName, fileSize, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    ByteSliceList* byteSliceList = NULL;
    ASSERT_EQ(nullptr, byteSliceList = blockFileNode->ReadToByteSliceList(len, offset, ReadOption()));
    delete byteSliceList;
}

void BlockFileNodeTest::TestCaseForReadEmptyFileWithCache()
{
    GenerateFile(_fileName, 0, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    const uint64_t len = 100;
    char buffer[len];
    ASSERT_EQ(FSEC_BADARGS, blockFileNode->Read(buffer, len, 0, ReadOption()).Code());
}

void BlockFileNodeTest::TestCaseForGetBlocks()
{
    _blockSize = 4096;
    GenerateFile(_fileName, 20 * _blockSize + 2, _blockSize);
    {
        BlockCacheOption option = util::BlockCacheOption::LRU(100 * _blockSize, _blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "1";
        _blockCache.reset(BlockCacheCreator::Create(option));
    }

    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    ReadOption option;
    {
        auto handles = future_lite::coro::syncAwait(blockFileNode->_accessor.GetBlockHandles({0, 1, 2, 3, 4}, option));
        for (size_t i = 0; i < 5; i++) {
            ASSERT_TRUE(handles[i].GetOrThrow().GetBlock() != NULL);
            const string expectData(_blockSize, 'a' + i);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetOrThrow().GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, _blockSize));
        }
    }
    {
        ASSERT_EQ(5, blockFileNode->GetBlockCache()->GetBlockCount());
        auto handles = future_lite::coro::syncAwait(blockFileNode->_accessor.GetBlockHandles({3, 4}, option));
        for (size_t i = 0; i < 2; i++) {
            ASSERT_TRUE(handles[i].GetOrThrow().GetBlock() != NULL);
            const string expectData(_blockSize, 'a' + i + 3);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetOrThrow().GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, _blockSize));
        }
    }
    {
        ASSERT_EQ(5, blockFileNode->GetBlockCache()->GetBlockCount());
        auto handles = future_lite::coro::syncAwait(blockFileNode->_accessor.GetBlockHandles({3, 4, 5, 6, 7}, option));
        for (size_t i = 0; i < 5; i++) {
            ASSERT_TRUE(handles[i].GetOrThrow().GetBlock() != NULL);
            const string expectData(_blockSize, 'a' + i + 3);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetOrThrow().GetBlock()->data);
            ASSERT_EQ(expectData, string(addr, _blockSize));
        }
        ASSERT_EQ(8, blockFileNode->GetBlockCache()->GetBlockCount());
    }
    {
        BlockHandle blockHandle;
        blockFileNode->_accessor.GetBlock(10 * _blockSize, blockHandle).GetOrThrow();
        const string expectData(_blockSize, 'a' + 10);
        const char* addr1 = reinterpret_cast<const char*>(blockHandle.GetBlock()->data);
        EXPECT_EQ(expectData, string(addr1, _blockSize));
        ASSERT_EQ(9, blockFileNode->GetBlockCache()->GetBlockCount());
        auto handles =
            future_lite::coro::syncAwait(blockFileNode->_accessor.GetBlockHandles({8, 9, 10, 11, 12}, option));
        ASSERT_EQ((size_t)5, handles.size());
        for (size_t i = 0; i < 5; i++) {
            ASSERT_TRUE(handles[i].GetOrThrow().GetBlock() != NULL);
            const string expectData(_blockSize, 'a' + i + 8);
            const char* addr = reinterpret_cast<const char*>(handles[i].GetOrThrow().GetBlock()->data);
            EXPECT_EQ(expectData, string(addr, _blockSize)) << "not equal " << i << endl;
        }
        ASSERT_EQ(13, blockFileNode->GetBlockCache()->GetBlockCount());
    }
    {
        BlockHandle blockHandle;
        ASSERT_EQ(blockFileNode->_accessor.GetBlock(9999 * _blockSize, blockHandle).Code(), FSEC_ERROR);
    }
}

void BlockFileNodeTest::TestCaseForGetBaseAddressException()
{
    GenerateFile(_fileName, 100, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));

    ASSERT_FALSE(blockFileNode->GetBaseAddress());
}

void BlockFileNodeTest::TestCaseForWriteException()
{
    GenerateFile(_fileName, 100, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode();
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));

    uint8_t buffer[] = "abc";
    ASSERT_EQ(FSEC_NOTSUP, blockFileNode->Write(buffer, 3).Code());
}

class FakeCommonFileWrapper : public FslibCommonFileWrapper
{
public:
    FakeCommonFileWrapper(const string& path)
        : FslibCommonFileWrapper(fslib::fs::FileSystem::openFile(path, fslib::READ))
    {
    }
    FSResult<void> PRead(void* buffer, size_t length, off_t offset, size_t& realLength) noexcept override
    {
        if (count == 1) {
            // INDEXLIB_THROW(util::FileIOException, "%s", "");
            return FSEC_ERROR;
        }
        ++count;
        realLength = length;
        return FSEC_OK;
    }

    future_lite::Future<FSResult<size_t>> PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                     future_lite::Executor* executor) noexcept override
    {
        future_lite::Promise<FSResult<size_t>> promise;
        auto future = promise.getFuture();
        MoveWrapper<decltype(promise)> p(std::move(promise));
        size_t realLength = 0;
        if (PRead(buffer, length, offset, realLength) != FSEC_OK) {
            p.get().setValue(FSResult<size_t> {FSEC_ERROR, 0});
        } else {
            p.get().setValue(FSResult<size_t> {FSEC_OK, realLength});
        }
        return future;
    }

private:
    int count = 0;
};

void BlockFileNodeTest::TestCaseForReadWithByteSliceListException()
{
    _blockSize = 4096;
    GenerateFile(_fileName, 4 * _blockSize, _blockSize);
    FakeCommonFileWrapper* fakeFileWrapper = new FakeCommonFileWrapper(_fileName);
    FslibFileWrapperPtr fileWrapper(fakeFileWrapper);
    BlockCacheOption option = BlockCacheOption::LRU(4 * _blockSize, _blockSize, _iOBatchSize);
    option.cacheParams["num_shard_bits"] = "1";
    _blockCache.reset(BlockCacheCreator::Create(option));

    BlockFileNode blockFileNode(_blockCache.get(), false, false, false, "");
    ASSERT_EQ(FSEC_OK, blockFileNode.Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    ASSERT_EQ(FSEC_OK, blockFileNode._accessor.Close());
    blockFileNode._accessor.TEST_SetFile(fileWrapper);

    char* buffer = new char[_blockSize * 4];

    ByteSliceList* byteSliceList = NULL;
    byteSliceList = blockFileNode.ReadToByteSliceList(3 * _blockSize, 0, ReadOption());
    ASSERT_EQ(0, _blockCache->GetBlockCount());
    ASSERT_EQ(0, fakeFileWrapper->count);

    ByteSliceReader reader;
    reader.Open(byteSliceList).GetOrThrow();
    ASSERT_EQ(1, _blockCache->GetBlockCount());
    ASSERT_EQ(1, fakeFileWrapper->count);

    ASSERT_THROW(reader.Read(buffer, _blockSize * 3).GetOrThrow(), FileIOException);
    ASSERT_EQ(1, fakeFileWrapper->count);
    ASSERT_EQ(1, _blockCache->GetBlockCount());

    delete byteSliceList;
    delete[] buffer;
}

void BlockFileNodeTest::GenerateFile(const std::string& fileName, size_t size, size_t blockSize)
{
    if (FslibWrapper::IsExist(fileName).GetOrThrow()) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(fileName, DeleteOption::NoFence(false)).Code());
    }
    auto file = FslibWrapper::OpenFile(fileName, fslib::WRITE).GetOrThrow();
    assert(file != NULL);
    char* buffer = new char[blockSize];
    char content = 0;
    size_t n = size / blockSize;
    size_t tail = size % blockSize;
    for (size_t i = 0; i < n; ++i) {
        memset(buffer, 'a' + content, blockSize);
        file->WriteE((void*)buffer, blockSize);
        content = (content + 1) % 26;
    }
    if (tail != 0) {
        memset(buffer, 'a' + content, tail);
        file->WriteE((void*)buffer, tail);
    }
    file->CloseE();
    delete[] buffer;
    buffer = NULL;
}

void BlockFileNodeTest::GenerateFile(const std::string& fileName, const std::string& content, size_t blockSize)
{
    if (FslibWrapper::IsExist(fileName).GetOrThrow()) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(fileName, DeleteOption::NoFence(false)).Code());
    }
    auto file = FslibWrapper::OpenFile(fileName, fslib::WRITE).GetOrThrow();
    assert(file != NULL);
    file->WriteE(content.data(), content.size());
    file->CloseE();
}

std::shared_ptr<BlockFileNode> BlockFileNodeTest::CreateFileNode(bool useDirectIO)
{
    auto option = BlockCacheOption::LRU(400 * _blockSize, _blockSize, _iOBatchSize);
    option.cacheParams["num_shard_bits"] = "1";
    _blockCache.reset(BlockCacheCreator::Create(option));

    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), useDirectIO, false, false, ""));
    return blockFileNode;
}

void BlockFileNodeTest::TestCaseForPrefetch()
{
    GenerateFile(_fileName, 10 * _blockSize + 1, _blockSize);
    {
        auto option = BlockCacheOption::LRU(1000 * _blockSize, _blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "1";
        _blockCache.reset(BlockCacheCreator::Create(option));
    }
    ReadOption option;
    option.blockCounter = new util::BlockAccessCounter();
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    const uint64_t len = 100;
    char buffer[len];
    ASSERT_EQ(0, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(0, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(20, blockFileNode->Prefetch(20, 0, option).GetOrThrow());
    ASSERT_EQ(0, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(5, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(20, blockFileNode->Read(buffer, 20, 0, option).GetOrThrow());
    ASSERT_EQ(5, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(5, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(4, blockFileNode->Read(buffer + 20, 4, 20, option).GetOrThrow());
    ASSERT_EQ(5, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(6, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(15, blockFileNode->Prefetch(15, 20, option).GetOrThrow());
    ASSERT_EQ(6, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(9, option.blockCounter->blockCacheMissCount);

    ASSERT_EQ(16, blockFileNode->Read(buffer + 24, 16, 24, option).GetOrThrow());
    ASSERT_EQ(9, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(10, option.blockCounter->blockCacheMissCount);
    ASSERT_EQ(string("aaaabbbbccccddddeeeeffffgggghhhhiiiijjjj"), string(buffer, 40));
    delete option.blockCounter;
}

void BlockFileNodeTest::TestCaseForNonContinuousMissBlock()
{
    _blockSize = 1024;
    uint32_t batchCount = 24;
    const uint32_t fileSize = batchCount * _blockSize;
    util::BlockAccessCounter blockAccessCounter;
    ReadOption option;
    option.blockCounter = &blockAccessCounter;
    GenerateFile(_fileName, fileSize, _blockSize);
    {
        auto option = BlockCacheOption::LRU(100 * _blockSize, _blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "1";
        _blockCache.reset(BlockCacheCreator::Create(option));
    }
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    char buffer[fileSize];
    uint32_t expectedMissCount = 0;
    uint32_t expectedHitCount = 0;
    for (int i = 0; i < fileSize; i += _blockSize * 4) {
        size_t length = std::min(size_t(_blockSize * 2), size_t(fileSize - i));
        ASSERT_EQ(length, blockFileNode->ReadAsync(buffer + i, length, i, option).get().GetOrThrow());
        expectedMissCount += 2;
        ASSERT_EQ(expectedMissCount, option.blockCounter->blockCacheMissCount);
        ASSERT_EQ(expectedHitCount, option.blockCounter->blockCacheHitCount);
    }
    expectedMissCount += batchCount / 2;
    expectedHitCount += batchCount / 2;
    ASSERT_EQ(fileSize, blockFileNode->ReadAsync(buffer, fileSize, 0, option).get().GetOrThrow());
    ASSERT_EQ(expectedHitCount, option.blockCounter->blockCacheHitCount);
    ASSERT_EQ(expectedMissCount, option.blockCounter->blockCacheMissCount);
    for (int i = 0, content = 0; i < fileSize; i += _blockSize, content = (content + 1) % 26) {
        ASSERT_EQ('a' + content, buffer[i]);
    }
}

void BlockFileNodeTest::TestCaseForReadUInt32Async()
{
    constexpr size_t blockSize = 2;
    constexpr uint8_t size = 21;
    string content(size, '\0');
    for (uint8_t i = 0; i < size; ++i) {
        content[i] = i;
    }
    GenerateFile(_fileName, content, blockSize);
    {
        auto option = BlockCacheOption::LRU(100 * blockSize, blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "0";
        _blockCache.reset(BlockCacheCreator::Create(option));
    }
    ReadOption option;
    option.blockCounter = new util::BlockAccessCounter();
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));

    auto check = [option, blockFileNode, &content](size_t offset) mutable {
        uint32_t result = blockFileNode->ReadUInt32Async(offset, option).get().GetOrThrow();
        uint32_t expected = *reinterpret_cast<uint32_t*>(const_cast<char*>(content.c_str()) + offset);
        ASSERT_EQ(expected, result) << "check " << offset << " failed";
    };
    check(0);
    check(5);
    check(13);
    check(17);

    delete option.blockCounter;
}

void BlockFileNodeTest::TestCaseForBatchReadMergeBlock()
{
    constexpr size_t blockSize = 4;
    constexpr size_t size = 21;
    string content(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        content[i] = i;
    }
    GenerateFile(_fileName, content, blockSize);
    auto option = BlockCacheOption::LRU(100 * blockSize, blockSize, _iOBatchSize);
    option.cacheParams["num_shard_bits"] = "0";
    _blockCache.reset(BlockCacheCreator::Create(option));
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));

    auto check = [blockFileNode, &content](BatchIO batchIO, size_t cacheVisitCount) mutable {
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        auto result = future_lite::coro::syncAwait(blockFileNode->BatchReadOrdered(batchIO, option));
        ASSERT_EQ(result.size(), batchIO.size());
        for (size_t i = 0; i < batchIO.size(); ++i) {
            ASSERT_TRUE(result[i].OK());
            ASSERT_EQ(batchIO[i].len, result[i].result);
            ASSERT_EQ(content.substr(batchIO[i].offset, batchIO[i].len),
                      string((char*)batchIO[i].buffer, batchIO[i].len));
        }
        EXPECT_EQ(cacheVisitCount, counter.blockCacheHitCount + counter.blockCacheMissCount);
    };
    char buffer[10][4096];
    size_t readLen = 2;

    {
        check({{buffer[0], readLen, 4}, {buffer[1], readLen, 8}}, 2);
        check({{buffer[0], 14, 1}}, 4); // two block hit cache, two read from file
    }

    check({{buffer[0], readLen, 0}, {buffer[1], readLen, 0}}, 1); // same offset
    check({{buffer[0], readLen, 0}, {buffer[1], readLen, 1}}, 1); // cross in one block
    check({{buffer[0], readLen, 0}, {buffer[1], readLen, 2}}, 1); // in one block

    check({{buffer[0], readLen, 0}, {buffer[1], readLen, 4}}, 2); // two blocks
    check({{buffer[0], 3, 0}, {buffer[1], 3, 3}}, 2);             // two blocks
}

void BlockFileNodeTest::TestCaseForMultiThreadBatchRead()
{
    size_t threadNum = 10;

    auto executor = util::FutureExecutor::CreateExecutor(threadNum * 2, 32);

    constexpr size_t blockSize = 4;
    constexpr size_t size = 4096;
    string content(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        content[i] = i % 128;
    }
    GenerateFile(_fileName, content, blockSize);
    {
        auto option = BlockCacheOption::LRU(0, blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "0";
        _blockCache.reset(BlockCacheCreator::Create(option));
    }
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    vector<autil::ThreadPtr> threads;
    atomic<bool> done(false);
    vector<size_t> loops(threadNum);
    for (size_t threadN = 0; threadN < threadNum; ++threadN) {
        threads.push_back(autil::Thread::createThread([&, idx = threadN]() {
            size_t loop = 0;
            while (!done) {
                loop++;
                BatchIO batchIO;
                size_t batchSize = 3;
                constexpr size_t blockCount = size / blockSize;
                char buffer[batchSize][blockSize];
                for (size_t i = 0; i < batchSize; ++i) {
                    batchIO.emplace_back(buffer[i], blockSize, random() % blockCount * blockSize);
                    ASSERT_EQ(
                        FSEC_OK,
                        blockFileNode->Read(batchIO[i].buffer, batchIO[i].len, batchIO[i].offset, ReadOption()).Code());
                }
                sort(batchIO.begin(), batchIO.end());
                auto readResult =
                    future_lite::coro::syncAwait(blockFileNode->BatchReadOrdered(batchIO, ReadOption()).via(executor));
                ASSERT_EQ(readResult.size(), batchSize);
                for (size_t i = 0; i < batchSize; ++i) {
                    ASSERT_EQ(blockSize, readResult[i].GetOrThrow());
                    ASSERT_EQ(content.substr(batchIO[i].offset, blockSize),
                              string((char*)batchIO[i].buffer, blockSize));
                }
            }
            loops[idx] = loop;
        }));
    }

    sleep(1);
    done = true;
    threads.clear();
    for (size_t i = 0; i < threadNum; ++i) {
        cout << "thread " << i << " run " << loops[i] << endl;
    }

    util::FutureExecutor::DestroyExecutor(executor);
}

void BlockFileNodeTest::TestCaseForBatchReadWithIOError()
{
    using ErrorGenerator = fslib::fs::ErrorGenerator;
    fslib::fs::FileSystem::_useMock = true;
    auto errorGenerator = ErrorGenerator::getInstance();
    ErrorGenerator::FileSystemErrorMap errorMap;
    fslib::fs::FileSystemError ec;
    ec.ec = fslib::ErrorCode::EC_BADARGS;
    ec.offset = 4096;

    errorMap.insert({{_fileName, "pread"}, ec});
    errorGenerator->setErrorMap(errorMap);
    constexpr size_t blockSize = 4096;
    constexpr size_t size = 4096 * 4;
    string content(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        content[i] = i % 128;
    }
    GenerateFile(_fileName, content, blockSize);
    {
        auto option = BlockCacheOption::LRU(100 * blockSize, blockSize, _iOBatchSize);
        option.cacheParams["num_shard_bits"] = "0";
        option.ioBatchSize = 1;
        _blockCache.reset(BlockCacheCreator::Create(option));
    }
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    char buffer[3][8192];
    BatchIO batchIO({{buffer[0], 4096, 0}, {buffer[1], 4096, 4096}, {buffer[2], 4096, 8192}});
    auto readResult = future_lite::coro::syncAwait(blockFileNode->BatchReadOrdered(batchIO, ReadOption()));
    ASSERT_EQ(batchIO.size(), readResult.size());
    ASSERT_EQ(4096, readResult[0].GetOrThrow());
    ASSERT_FALSE(readResult[1].OK());
    ASSERT_FALSE(readResult[1].OK());
    ASSERT_EQ(content.substr(0, 4096), string(buffer[0], 4096));
    fslib::fs::FileSystem::_useMock = false;
    errorGenerator->setErrorMap({});
}

// #define STACK_ARRAY_SIZE 8363500
namespace {
static constexpr size_t STACK_ARRAY_SIZE = 8300000;
} // namespace

void BlockFileNodeTest::TestMaxStackSize()
{
    char stack[STACK_ARRAY_SIZE] = {0};
    size_t sum = 0;
    for (size_t i = 0; i < STACK_ARRAY_SIZE; ++i) {
        sum += (int)stack[i];
    }
    cout << sum << endl;
}
}} // namespace indexlib::file_system
