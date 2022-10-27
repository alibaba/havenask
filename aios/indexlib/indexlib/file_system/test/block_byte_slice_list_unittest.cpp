#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/block_byte_slice_list.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/block_allocator.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);

class BlockByteSliceListTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BlockByteSliceListTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForReleaseAllBlocksInCache()
    {
        const uint32_t BLOCK_SIZE = 4;
        const uint32_t BLOCK_COUNT = 10;

        BlockCache blockCache;
        blockCache.Init(10 * BLOCK_SIZE, BLOCK_SIZE, 0);
        BlockAllocatorPtr allocator = blockCache.GetBlockAllocator();
        vector<Cache::Handle*> handleVec;
        
        for (uint32_t i = 0; i < BLOCK_COUNT; i++)
        {
            Cache::Handle* handle;
            Block *block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            ASSERT_TRUE(blockCache.Put(block, &handle));
            ASSERT_EQ(2U, blockCache.TEST_GetRefCount(handle));
            handleVec.push_back(handle);
        }
        ASSERT_EQ(10U, blockCache.GetBlockCount());
        for (uint32_t i = 0; i < BLOCK_COUNT; i++)
        {
            Cache::Handle* handle;
            Block* block = blockCache.Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block);
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(3U, blockCache.TEST_GetRefCount(handle));
            blockCache.ReleaseHandle(handle);
        }
        for (auto& handle : handleVec)
        {
            blockCache.ReleaseHandle(handle);
        }
        handleVec.clear();
        
        for (uint32_t i = 0; i < BLOCK_COUNT; i++)
        {
            Cache::Handle* handle;
            Block* block = blockCache.Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block) << i;
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(2U, blockCache.TEST_GetRefCount(handle));
            blockCache.ReleaseHandle(handle);
        }
    }

    void TestCaseForOverCacheSize()
    {
        const uint32_t BLOCK_SIZE = 4;
        const uint32_t BLOCK_COUNT = 10;

        BlockCache blockCache;
        blockCache.Init(BLOCK_COUNT * BLOCK_SIZE, BLOCK_SIZE, 0);
        BlockAllocatorPtr allocator = blockCache.GetBlockAllocator();
        vector<Cache::Handle*> handleVec;
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++)
        {
            Cache::Handle* handle;
            Block *block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            ASSERT_TRUE(blockCache.Put(block, &handle));
            ASSERT_EQ(2U, blockCache.TEST_GetRefCount(handle));
            handleVec.push_back(handle);
        }
        ASSERT_EQ(10U, blockCache.GetMaxBlockCount());
        ASSERT_EQ(20U, blockCache.GetBlockCount());
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++)
        {
            Cache::Handle* handle;
            Block* block = blockCache.Get(blockid_t(0, i), &handle);
            ASSERT_TRUE(block);
            ASSERT_EQ(i, block->id.inFileIdx);
            ASSERT_EQ(3U, blockCache.TEST_GetRefCount(handle));
            blockCache.ReleaseHandle(handle);
        }
        for (auto& handle : handleVec)
        {
            blockCache.ReleaseHandle(handle);
        }
        handleVec.clear();
        ASSERT_EQ(10U, blockCache.GetBlockCount());
        uint32_t notInCacheCount = 0;
        for (uint32_t i = 0; i < BLOCK_COUNT * 2; i++)
        {
            Cache::Handle* handle;
            Block* block = blockCache.Get(blockid_t(0, i), &handle);
            if (block)
            {
                ASSERT_TRUE(block) << i;
                ASSERT_EQ(i, block->id.inFileIdx);
                ASSERT_EQ(2U, blockCache.TEST_GetRefCount(handle));
                blockCache.ReleaseHandle(handle);
            }
            else
            {
                notInCacheCount ++;
            }
        }
        ASSERT_EQ(10U, notInCacheCount);
    }

    void TestCaseForSplitSlice()
    {
        auto assertSlice = [this](util::ByteSlice* slice, size_t size, size_t dataSize, size_t offset) {
            ASSERT_EQ(size, slice->size);
            ASSERT_EQ(dataSize, slice->dataSize);
            ASSERT_EQ(offset, slice->offset);
        };

        auto accessor = prepareBlockFileAccessor(4, 40);
        {
            BlockByteSliceList sliceList(accessor);
            sliceList.AddBlock(0, 40);
            auto head = sliceList.GetHead();
            assertSlice(head, 40, 0, 0);
            sliceList.GetSlice(0, head);
            assertSlice(head, 40, 4, 0);
            auto headNext = sliceList.GetNextSlice(head);
            assertSlice(headNext, 36, 4, 4);
            ASSERT_EQ(headNext, static_cast<util::ByteSlice*>(head->next));
            auto slice = sliceList.GetSlice(39, headNext);
            ASSERT_EQ(slice, static_cast<util::ByteSlice*>(headNext->next));            
            assertSlice(slice, 4, 4, 36);
            assertSlice(headNext, 32, 4, 4);

            // before: headNext is  [4, 36)
            slice = sliceList.GetSlice(15, headNext);
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
            BlockByteSliceList sliceList(accessor);
            sliceList.AddBlock(2, 47);  // end pos is offset 49
            auto head = sliceList.GetHead();
            assertSlice(head, 47, 0, 2);
            auto ret = sliceList.GetSlice(2, head);
            ASSERT_EQ(head, ret);            
            assertSlice(head, 47, 2, 2);
            auto headNext = sliceList.GetNextSlice(head);
            assertSlice(headNext, 45, 4, 4);
            ASSERT_EQ(headNext, static_cast<util::ByteSlice*>(head->next));
            auto slice = sliceList.GetSlice(48, headNext);
            ASSERT_EQ(slice, static_cast<util::ByteSlice*>(headNext->next));
            assertSlice(slice, 1, 1, 48);
            assertSlice(headNext, 44, 4, 4);
            ASSERT_EQ(nullptr, sliceList.GetNextSlice(slice));
        }
        destoryBlockFileAccessor(accessor);

        accessor = prepareBlockFileAccessor(100, 80);
        {
            // file [0, 80)
            // last block [10, 60)
            // accessor range [10, 60)            
            BlockByteSliceList sliceList(accessor);
            sliceList.AddBlock(10, 50);  // end pos is offset 60
            auto head = sliceList.GetHead();
            assertSlice(head, 50, 0, 10);
            auto ret = sliceList.GetSlice(10, head);
            ASSERT_EQ(head, ret);
            ret = sliceList.GetSlice(10, head); // repeat get will return same result
            ASSERT_EQ(head, ret);            
            assertSlice(head, 50, 50, 10);
            auto headNext = sliceList.GetNextSlice(head);
            ASSERT_EQ(nullptr, headNext);
        }
        destoryBlockFileAccessor(accessor);        
        
    }

    BlockFileAccessor* prepareBlockFileAccessor(
        size_t blockSize, size_t fileLength)
    {
        size_t blockCount = (fileLength / blockSize) + 1;
        auto blockCache = new BlockCache();
        blockCache->Init(1000 * blockSize, blockSize, 0);
        BlockAllocatorPtr allocator = blockCache->GetBlockAllocator();
        for (uint32_t i = 0; i < blockCount; i++)
        {
            Cache::Handle* handle = nullptr;
            Block *block = allocator->AllocBlock();
            block->id = blockid_t(0, i);
            bool ret = blockCache->Put(block, &handle);
            (void)ret;
            assert(ret);
            assert(2U == blockCache->TEST_GetRefCount(handle));
            blockCache->ReleaseHandle(handle);
        }
        assert(blockCount == blockCache->GetBlockCount());
        auto accessor = new BlockFileAccessor(blockCache, true);
        accessor->mFileId = 0;
        accessor->mFileLength = fileLength;
        accessor->mFileBeginOffset = 0;
        return accessor;
    }
    void destoryBlockFileAccessor(BlockFileAccessor* accessor)
    {
        delete accessor->mBlockCache;
        accessor->mBlockCache = nullptr;
        delete accessor;
    }
};

INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForReleaseAllBlocksInCache);
INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForOverCacheSize);
INDEXLIB_UNIT_TEST_CASE(BlockByteSliceListTest, TestCaseForSplitSlice);

IE_NAMESPACE_END(file_system);
