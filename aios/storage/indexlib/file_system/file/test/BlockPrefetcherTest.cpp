#include "indexlib/file_system/file/BlockPrefetcher.h"

#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/cache/MemoryBlockCache.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BlockPrefetcherTest : public INDEXLIB_TESTBASE
{
public:
    BlockPrefetcherTest();
    ~BlockPrefetcherTest();

    DECLARE_CLASS_NAME(BlockPrefetcherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    std::shared_ptr<BlockFileNode> GenerateFileNode(const std::string& content);

private:
    size_t _blockSize;
    std::string _rootDir;
    std::string _fileName;
    util::BlockCachePtr _blockCache;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BlockPrefetcherTest);

INDEXLIB_UNIT_TEST_CASE(BlockPrefetcherTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

BlockPrefetcherTest::BlockPrefetcherTest() {}

BlockPrefetcherTest::~BlockPrefetcherTest() {}

void BlockPrefetcherTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _fileName = _rootDir + "block_file";
    _blockCache.reset(new util::MemoryBlockCache);
    _blockSize = 4;
    _blockCache->Init(BlockCacheOption::LRU(4096, _blockSize, 4));
}

void BlockPrefetcherTest::CaseTearDown() {}

void BlockPrefetcherTest::TestSimpleProcess()
{
    std::shared_ptr<BlockFileNode> fileNode = GenerateFileNode("aaaabbbbccccddddeeeefff");
    auto accessor = fileNode->GetAccessor();
    BlockPrefetcher prefetcher(accessor, ReadOption());
    auto CheckPrefetch = [&accessor](BlockPrefetcher& prefetcher, size_t offset, size_t length, string content) {
        ASSERT_EQ(length, content.length());
        ASSERT_EQ(ErrorCode::FSEC_OK, future_lite::coro::syncAwait(prefetcher.Prefetch(offset, length)));
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        string buffer(content.size(), '-');
        ASSERT_EQ(content.size(), accessor->Read(buffer.data(), length, offset, option).GetOrThrow());
        ASSERT_EQ(content, buffer);
        ASSERT_EQ(0, counter.blockCacheMissCount);
    };

    CheckPrefetch(prefetcher, 0, 4, "aaaa");
    CheckPrefetch(prefetcher, 20, 3, "fff");
    CheckPrefetch(prefetcher, 1, 6, "aaabbb");
}

std::shared_ptr<BlockFileNode> BlockPrefetcherTest::GenerateFileNode(const std::string& content)
{
    if (FslibWrapper::IsExist(_fileName).GetOrThrow()) {
        FslibWrapper::DeleteFileE(_fileName, DeleteOption::NoFence(false));
    }
    auto file = FslibWrapper::OpenFile(_fileName, fslib::WRITE).GetOrThrow();
    auto ret = file->Write((void*)content.data(), content.size());
    (void)ret;
    auto ec = file->Close().Code();
    (void)ec;
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), false, false, false, ""));
    EXPECT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));
    return blockFileNode;
}
}} // namespace indexlib::file_system
