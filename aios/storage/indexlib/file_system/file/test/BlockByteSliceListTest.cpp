#include "indexlib/file_system/file/BlockByteSliceList.h"

#include "gtest/gtest.h"
#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BlockByteSliceListTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BlockByteSliceListTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForReleaseAllBlocksInCache()
    {
        const uint32_t BLOCK_SIZE = 40960;
        const uint32_t IO_BATCH_SIZE = 4;
        const uint32_t BLOCK_COUNT = 10;

        auto option = BlockCacheOption::LRU(20 * BLOCK_SIZE, BLOCK_SIZE, IO_BATCH_SIZE);
        option.cacheParams["num_shard_bits"] = "0";
        BlockCachePtr blockCache(BlockCacheCreator::Create(option));
        BlockAllocatorPtr allocator = blockCache->GetBlockAllocator();

        vector<CacheBase::Handle*> handleVec;

        for (uint32_t i = 0; i < BLOCK_COUNT; i++) {
            CacheBase::Handle* handle;
            Block* block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            ASSERT_TRUE(blockCache->Put(block, &handle, autil::CacheBase::Priority::LOW));
            ASSERT_EQ(2U, blockCache->TEST_GetRefCount(handle));
            handleVec.push_back(handle);
        }
        ASSERT_EQ(10U, blockCache->GetBlockCount());
        for (uint32_t i = 0; i < BLOCK_COUNT; i++) {
            CacheBase::Handle* handle;
            Block* block = blockCache->Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block);
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(3U, blockCache->TEST_GetRefCount(handle));
            blockCache->ReleaseHandle(handle);
        }
        for (auto& handle : handleVec) {
            blockCache->ReleaseHandle(handle);
        }
        handleVec.clear();

        for (uint32_t i = 0; i < BLOCK_COUNT; i++) {
            CacheBase::Handle* handle;
            Block* block = blockCache->Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block) << i;
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(2U, blockCache->TEST_GetRefCount(handle));
            blockCache->ReleaseHandle(handle);
        }
    }

    void TestCaseForOverCacheSize()
    {
        const uint32_t BLOCK_SIZE = 40960;
        const uint32_t IO_BATCH_SIZE = 4;
        const uint32_t BLOCK_COUNT = 10;

        auto option = BlockCacheOption::LRU((BLOCK_COUNT + 1) * BLOCK_SIZE, BLOCK_SIZE, IO_BATCH_SIZE);
        option.cacheParams["num_shard_bits"] = "0";
        BlockCachePtr blockCache(BlockCacheCreator::Create(option));
        BlockAllocatorPtr allocator = blockCache->GetBlockAllocator();

        vector<CacheBase::Handle*> handleVec;
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++) {
            CacheBase::Handle* handle;
            Block* block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            ASSERT_TRUE(blockCache->Put(block, &handle, autil::CacheBase::Priority::LOW));
            ASSERT_EQ(2U, blockCache->TEST_GetRefCount(handle));
            handleVec.push_back(handle);
        }
        ASSERT_EQ(11U, blockCache->GetMaxBlockCount());
        ASSERT_EQ(20U, blockCache->GetBlockCount());
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++) {
            CacheBase::Handle* handle;
            Block* block = blockCache->Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block);
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(3U, blockCache->TEST_GetRefCount(handle));
            blockCache->ReleaseHandle(handle);
        }
        for (auto& handle : handleVec) {
            blockCache->ReleaseHandle(handle);
        }
        handleVec.clear();
        ASSERT_EQ(10U, blockCache->GetBlockCount());
        uint32_t notInCacheCount = 0;
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++) {
            CacheBase::Handle* handle;
            Block* block = blockCache->Get(blockid_t(0, i), &handle);
            if (block) {
                ASSERT_TRUE(block) << i;
                ASSERT_EQ(i, block->id.inFileIdx);
                ASSERT_EQ(2U, blockCache->TEST_GetRefCount(handle));
                blockCache->ReleaseHandle(handle);
            } else {
                notInCacheCount++;
            }
        }
        ASSERT_EQ(10U, notInCacheCount);
    }

    void TestCaseForSplitSlice()
    {
        auto assertSlice = [](util::ByteSlice* slice, size_t size, size_t dataSize, size_t offset) {
            ASSERT_EQ(size, slice->size);
            ASSERT_EQ(dataSize, slice->dataSize);
            ASSERT_EQ(offset, slice->offset);
        };

        auto accessor = prepareBlockFileAccessor(4, 40);
        {
            ReadOption option;
            BlockByteSliceList sliceList(option, accessor);
            sliceList.AddBlock(0, 40);
            auto head = sliceList.GetHead();
            assertSlice(head, 40, 0, 0);
            sliceList.GetSlice(0, head).GetOrThrow();
            assertSlice(head, 40, 4, 0);
            auto headNext = sliceList.GetNextSlice(head).GetOrThrow();
            assertSlice(headNext, 36, 4, 4);
            ASSERT_EQ(headNext, static_cast<util::ByteSlice*>(head->next));
            auto slice = sliceList.GetSlice(39, headNext).GetOrThrow();
            ASSERT_EQ(slice, static_cast<util::ByteSlice*>(headNext->next));
            assertSlice(slice, 4, 4, 36);
            assertSlice(headNext, 32, 4, 4);

            // before: headNext is  [4, 36)
            slice = sliceList.GetSlice(15, headNext).GetOrThrow();
            // after: headNext should be [4, 12)
            //  and new slice should be [12, 36)
            ASSERT_EQ(slice, static_cast<util::ByteSlice*>(headNext->next));
            assertSlice(headNext, 8, 4, 4);
            assertSlice(slice, 24, 4, 12);
        }
        destoryBlockFileAccessor(accessor);

        accessor = prepareBlockFileAccessor(4, 50);
        {
            // file [0, 50)
            // last block [48, 52)
            // accessor range [2, 49)
            ReadOption option;
            BlockByteSliceList sliceList(option, accessor);
            sliceList.AddBlock(2, 47); // end pos is offset 49
            auto head = sliceList.GetHead();
            assertSlice(head, 47, 0, 2);
            auto ret = sliceList.GetSlice(2, head).GetOrThrow();
            ASSERT_EQ(head, ret);
            assertSlice(head, 47, 2, 2);
            auto headNext = sliceList.GetNextSlice(head).GetOrThrow();
            assertSlice(headNext, 45, 4, 4);
            ASSERT_EQ(headNext, static_cast<util::ByteSlice*>(head->next));
            auto slice = sliceList.GetSlice(48, headNext).GetOrThrow();
            ASSERT_EQ(slice, static_cast<util::ByteSlice*>(headNext->next));
            assertSlice(slice, 1, 1, 48);
            assertSlice(headNext, 44, 4, 4);
            ASSERT_EQ(nullptr, sliceList.GetNextSlice(slice).GetOrThrow());
        }
        destoryBlockFileAccessor(accessor);

        accessor = prepareBlockFileAccessor(100, 80);
        {
            // file [0, 80)
            // last block [10, 60)
            // accessor range [10, 60)
            ReadOption option;
            BlockByteSliceList sliceList(option, accessor);
            sliceList.AddBlock(10, 50); // end pos is offset 60
            auto head = sliceList.GetHead();
            assertSlice(head, 50, 0, 10);
            auto ret = sliceList.GetSlice(10, head).GetOrThrow();
            ASSERT_EQ(head, ret);
            ret = sliceList.GetSlice(10, head).GetOrThrow(); // repeat get will return same result
            ASSERT_EQ(head, ret);
            assertSlice(head, 50, 50, 10);
            auto headNext = sliceList.GetNextSlice(head).GetOrThrow();
            ASSERT_EQ(nullptr, headNext);
        }
        destoryBlockFileAccessor(accessor);
    }

    BlockFileAccessor* prepareBlockFileAccessor(size_t blockSize, size_t fileLength)
    {
        size_t blockCount = (fileLength / blockSize) + 1;
        size_t ioBatchSize = 4;
        auto option = BlockCacheOption::LRU(1000 * blockSize, blockSize, ioBatchSize);
        option.cacheParams["num_shard_bits"] = "0";
        BlockCache* blockCache = BlockCacheCreator::Create(option);
        BlockAllocatorPtr allocator = blockCache->GetBlockAllocator();

        for (uint32_t i = 0; i < blockCount; i++) {
            CacheBase::Handle* handle = nullptr;
            Block* block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            bool ret = blockCache->Put(block, &handle, autil::CacheBase::Priority::LOW);
            (void)ret;
            assert(ret);
            assert(2U == blockCache->TEST_GetRefCount(handle));
            blockCache->ReleaseHandle(handle);
        }
        // because each block usage is bigger than block size(consider of lru handle size)
        assert(blockCount <= blockCache->GetBlockCount());
        auto accessor = new BlockFileAccessor(blockCache, true, false, "");
        accessor->_fileId = 0;
        accessor->_fileLength = fileLength;
        accessor->_fileBeginOffset = 0;
        return accessor;
    }
    void destoryBlockFileAccessor(BlockFileAccessor* accessor)
    {
        delete accessor->_blockCache;
        accessor->_blockCache = nullptr;
        delete accessor;
    }
};

INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForReleaseAllBlocksInCache);
INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForOverCacheSize);
INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForSplitSlice);
}} // namespace indexlib::file_system
