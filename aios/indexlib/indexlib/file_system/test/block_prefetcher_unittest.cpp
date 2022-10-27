#include "indexlib/file_system/test/block_prefetcher_unittest.h"
#include "indexlib/file_system/block_file_accessor.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockPrefetcherTest);

BlockPrefetcherTest::BlockPrefetcherTest()
{
}

BlockPrefetcherTest::~BlockPrefetcherTest()
{
}

void BlockPrefetcherTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mFileName = mRootDir + "block_file";
    mBlockCache.reset(new util::BlockCache);
    mBlockSize = 4;
    mBlockCache->Init(4096, mBlockSize, 1, false, true);
}

void BlockPrefetcherTest::CaseTearDown()
{
}

void BlockPrefetcherTest::TestSimpleProcess()
{
    BlockFileNodePtr fileNode = GenerateFileNode("aaaabbbbccccddddeeeefff");
    auto accessor = fileNode->GetAccessor();
    BlockPrefetcher prefetcher(accessor);

    //first prefetch, do not prefetch
    CheckReadAhead(prefetcher, 0, 20, Range(1, 1), Range(1, 1), "aaaa");
    //read a empty pos, pefetch a neighboring block
    CheckReadAhead(prefetcher, 4, 20, Range(2, 2), Range(2, 3), "bbbb");
    //read a pos in future, wait future finish and pefetch two neighboring block
    CheckReadAhead(prefetcher, 8, 20, Range(2, 3), Range(2, 5), "cccc");
    //read a pos in future, which is not end. do not prefetch
    CheckReadAhead(prefetcher, 12, 20, Range(2, 5), Range(2, 5), "dddd");
    //read a pos in cache, which is tha last block in cache. so prefetch the last block in slice
    CheckReadAhead(prefetcher, 16, 23, Range(2, 5), Range(2, 6), "eeee");
    //read a pos in future
    CheckReadAhead(prefetcher, 20, 23, Range(2, 6), Range(2, 6), "fff");
}

void BlockPrefetcherTest::TestReadASync()
{
    BlockFileNodePtr fileNode = GenerateFileNode("aaaabbbbccccddddeeeefff");
    auto accessor = fileNode->GetAccessor();
    BlockPrefetcher prefetcher(accessor);

    CheckReadAhead(prefetcher, 0, 20, Range(1, 1), Range(1, 1), "aaaa");
    CheckReadAhead(prefetcher, 12, 20, Range(4, 4), Range(4, 5), "dddd");
    prefetcher.ReadAhead(0, 20);
    ASSERT_EQ(Range(4, 4), prefetcher.TEST_GetCacheRange());
    ASSERT_EQ(Range(4, 5), prefetcher.TEST_GetFutureRange());
    prefetcher.ReadAhead(16, 25);
    ASSERT_EQ(Range(4, 5), prefetcher.TEST_GetCacheRange());
    ASSERT_EQ(Range(4, 7), prefetcher.TEST_GetFutureRange());
}

void BlockPrefetcherTest::CheckReadAhead(
        BlockPrefetcher& prefetcher,
        size_t pos, size_t maxOffset,
        pair<size_t, size_t> cacheRange,
        pair<size_t, size_t> futureRange,
        const string& content)
{
    util::Cache::Handle* handle;
    Block* block = prefetcher.ReadAhead(pos, maxOffset, &handle);
    size_t blockDataSize = std::min(mBlockSize, maxOffset - pos);
    ASSERT_EQ(string(reinterpret_cast<char*>(block->data), blockDataSize), content);
    ASSERT_EQ(cacheRange, prefetcher.TEST_GetCacheRange());
    ASSERT_EQ(futureRange, prefetcher.TEST_GetFutureRange());
    mBlockCache->ReleaseHandle(handle);
}

BlockFileNodePtr BlockPrefetcherTest::GenerateFileNode(
        const std::string& content)
{
    if (storage::FileSystemWrapper::IsExist(mFileName))
    {
        storage::FileSystemWrapper::DeleteFile(mFileName);
    }
    storage::FileWrapper* file = storage::FileSystemWrapper::OpenFile(mFileName, fslib::WRITE);
    file->Write((void*)content.data(), content.size());
    file->Close();
    delete file;
    file_system::BlockFileNodePtr blockFileNode(
            new file_system::BlockFileNode(mBlockCache.get(), false));
    blockFileNode->Open(mFileName, file_system::FSOT_CACHE);
    return blockFileNode;
}

IE_NAMESPACE_END(file_system);

