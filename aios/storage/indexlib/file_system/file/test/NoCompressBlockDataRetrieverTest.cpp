#include "indexlib/file_system/file/NoCompressBlockDataRetriever.h"

#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/cache/MemoryBlockCache.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class NoCompressBlockDataRetrieverTest : public INDEXLIB_TESTBASE
{
public:
    NoCompressBlockDataRetrieverTest();
    ~NoCompressBlockDataRetrieverTest();

    DECLARE_CLASS_NAME(NoCompressBlockDataRetrieverTest);

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
AUTIL_LOG_SETUP(indexlib.file_system, NoCompressBlockDataRetrieverTest);

INDEXLIB_UNIT_TEST_CASE(NoCompressBlockDataRetrieverTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

NoCompressBlockDataRetrieverTest::NoCompressBlockDataRetrieverTest() {}

NoCompressBlockDataRetrieverTest::~NoCompressBlockDataRetrieverTest() {}

void NoCompressBlockDataRetrieverTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _fileName = _rootDir + "block_file";
    _blockCache.reset(new util::MemoryBlockCache);
    _blockSize = 4;
    _blockCache->Init(BlockCacheOption::LRU(4096, _blockSize, 4));
}

void NoCompressBlockDataRetrieverTest::CaseTearDown() {}

void NoCompressBlockDataRetrieverTest::TestSimpleProcess()
{
    std::shared_ptr<BlockFileNode> fileNode = GenerateFileNode("aaaabbbbccccddddeeeefff");
    auto accessor = fileNode->GetAccessor();
    ReadOption readOption;
    NoCompressBlockDataRetriever dataRetriever(readOption, accessor);
    auto CheckPrefetch = [&accessor](NoCompressBlockDataRetriever& retriever, size_t offset, size_t length,
                                     string content) {
        ASSERT_EQ(length, content.length());
        ASSERT_EQ(ErrorCode::FSEC_OK, future_lite::coro::syncAwait(retriever.Prefetch(offset, length)));
        util::BlockAccessCounter counter;
        ReadOption option;
        option.blockCounter = &counter;
        string buffer(content.size(), '-');
        ASSERT_EQ(content.size(), accessor->Read(buffer.data(), length, offset, option).GetOrThrow());
        ASSERT_EQ(content, buffer);
        ASSERT_EQ(0, counter.blockCacheMissCount);
        ASSERT_TRUE(counter.blockCacheHitCount > 0);
    };

    CheckPrefetch(dataRetriever, 0, 4, "aaaa");
    CheckPrefetch(dataRetriever, 20, 3, "fff");
    CheckPrefetch(dataRetriever, 1, 6, "aaabbb");
}

std::shared_ptr<BlockFileNode> NoCompressBlockDataRetrieverTest::GenerateFileNode(const std::string& content)
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
