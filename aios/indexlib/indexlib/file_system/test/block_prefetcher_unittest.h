#ifndef __INDEXLIB_BLOCKPREFETCHERTEST_H
#define __INDEXLIB_BLOCKPREFETCHERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/block_prefetcher.h"
#include "indexlib/file_system/block_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

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
    void TestReadASync();
private:
    using Range = std::pair<size_t, size_t>;
    void CheckReadAhead(
        BlockPrefetcher& prefetcher,
        size_t pos, size_t maxOffset,
        std::pair<size_t, size_t> cacheRange,
        std::pair<size_t, size_t> futureRange,
        const std::string& content);

    
    BlockFileNodePtr GenerateFileNode(
        const std::string& content);

private:
    size_t mBlockSize;
    std::string mRootDir;
    std::string mFileName;
    util::BlockCachePtr mBlockCache;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockPrefetcherTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BlockPrefetcherTest, TestReadASync);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCKPREFETCHERTEST_H
