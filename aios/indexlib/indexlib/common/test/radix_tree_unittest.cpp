#include "indexlib/common/test/radix_tree_unittest.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RadixTreeTest);

RadixTreeTest::RadixTreeTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
}

RadixTreeTest::~RadixTreeTest()
{
    delete mByteSlicePool;
}

void RadixTreeTest::CaseSetUp()
{
}

void RadixTreeTest::CaseTearDown()
{
    mByteSlicePool->release();
}

void RadixTreeTest::TestCalculatePowerOf2()
{
    ASSERT_EQ((uint8_t)0, RadixTree::CalculatePowerOf2(1));
    ASSERT_EQ((uint8_t)1, RadixTree::CalculatePowerOf2(2));
    ASSERT_EQ((uint8_t)2, RadixTree::CalculatePowerOf2(3));
    ASSERT_EQ((uint8_t)2, RadixTree::CalculatePowerOf2(4));
    ASSERT_EQ((uint8_t)3, RadixTree::CalculatePowerOf2(5));
}

void RadixTreeTest::TestNeedGrowUp()
{
    RadixTree tree(2, 2, mByteSlicePool);
    tree.mSliceNum = 1;
    ASSERT_FALSE(tree.NeedGrowUp(0));
    tree.mSliceNum = 2;
    ASSERT_TRUE(tree.NeedGrowUp(0));
    ASSERT_FALSE(tree.NeedGrowUp(1));
    tree.mSliceNum = 4;
    ASSERT_TRUE(tree.NeedGrowUp(1));
    ASSERT_FALSE(tree.NeedGrowUp(2));
    tree.mSliceNum = 8;
    ASSERT_TRUE(tree.NeedGrowUp(2));
}

void RadixTreeTest::TestCalculateSliceId()
{
    {
        RadixTree tree(2, 2, mByteSlicePool);
        ASSERT_EQ((uint64_t)2, tree.CalculateSliceId(4));
    }
    {
        RadixTree tree(64, 128, mByteSlicePool);
        ASSERT_EQ((uint64_t)32, tree.CalculateSliceId(4096));
        ASSERT_EQ((uint64_t)(10000/128), tree.CalculateSliceId(10000));
    }

    //test with unit size
    {
        RadixTree tree(2, 2, mByteSlicePool, 2);
        ASSERT_EQ((uint64_t)2, tree.CalculateSliceId(4));
        ASSERT_EQ((uint64_t)1, tree.CalculateSliceId(3));
    }
    
    {
        RadixTree tree(64, 128, mByteSlicePool, 2);
        ASSERT_EQ((uint64_t)32, tree.CalculateSliceId(4096));
        ASSERT_EQ((uint64_t)(10000/128), tree.CalculateSliceId(10000));
    }
}

void RadixTreeTest::TestAllocateConsecutiveSlices()
{
    RadixTree tree(2, 2, mByteSlicePool);
    tree.AllocateConsecutiveSlices(1, 2);
    ASSERT_EQ((uint32_t)1, tree.GetSliceNum());

    uint8_t* pointer = tree.AllocateConsecutiveSlices(3, 2);
    ASSERT_EQ((uint32_t)4, tree.GetSliceNum());
    uint8_t* slicePointer = tree.GetSlice(1);
    ASSERT_EQ(pointer, slicePointer);

    pointer += 2;
    slicePointer = tree.GetSlice(2);
    ASSERT_EQ(pointer, slicePointer);

    pointer += 2;
    slicePointer = tree.GetSlice(3);
    ASSERT_EQ(pointer, slicePointer);

    pointer = tree.AllocateConsecutiveSlices(0, 2);
    ASSERT_EQ(NULL, pointer);
}

void RadixTreeTest::TestAllocate()
{
    {
        RadixTree tree(2, 2, mByteSlicePool);
    
        //special case: allocate 0, allocate memory, can get current offfset
        uint8_t* pointer = tree.Allocate(0);
        ASSERT_EQ((uint32_t)1, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)0, tree.GetCurrentOffset());
        
        //normal allocate
        pointer = tree.Allocate(1);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)1, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)1, tree.GetCurrentOffset());
        
        //allocate length > left size
        pointer = tree.Allocate(2);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)2, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)4, tree.GetCurrentOffset());
        
        //allocate continues memory
        pointer = tree.Allocate(4);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)4, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)8, tree.GetCurrentOffset());
    }

    {
        RadixTree tree(2, 2, mByteSlicePool, 2);
        
        //special case: allocate 0, allocate memory, can get current offfset
        uint8_t* pointer = tree.Allocate(0);
        ASSERT_EQ((uint32_t)1, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)0, tree.GetCurrentOffset());
        
        //normal allocate
        pointer = tree.Allocate(1);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)1, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)1, tree.GetCurrentOffset());
        
        //allocate length > left size
        pointer = tree.Allocate(4);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)2, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)8, tree.GetCurrentOffset());
        
        //allocate continues memory
        pointer = tree.Allocate(8);
        ASSERT_TRUE(pointer);
        ASSERT_EQ((uint32_t)4, tree.GetSliceNum());
        ASSERT_EQ((uint32_t)16, tree.GetCurrentOffset());
    }
}

void RadixTreeTest::TestCalculateIdxInSlice()
{
    {
        RadixTree tree(2, 2, mByteSlicePool);
        ASSERT_EQ((uint8_t)1, tree.mPowerOfItemNum);
        ASSERT_EQ((uint64_t)0, tree.CalculateIdxInSlice(4));
    }
    {
        RadixTree tree(64, 128, mByteSlicePool);
        ASSERT_EQ((uint8_t)7, tree.mPowerOfItemNum);
        ASSERT_EQ((uint64_t)0, tree.CalculateIdxInSlice(4096));
        ASSERT_EQ((uint64_t)(4097%128), tree.CalculateIdxInSlice(4097));
        ASSERT_EQ((uint64_t)(10000%128), tree.CalculateIdxInSlice(10000));
    }

    //test with unit size
    {
        RadixTree tree(2, 2, mByteSlicePool, 2);
        ASSERT_EQ((uint8_t)1, tree.mPowerOfItemNum);
        ASSERT_EQ((uint64_t)0, tree.CalculateIdxInSlice(4));
        ASSERT_EQ((uint64_t)1, tree.CalculateIdxInSlice(3));
    }
    {
        RadixTree tree(64, 64, mByteSlicePool, 2);
        ASSERT_EQ((uint8_t)6, tree.mPowerOfItemNum);
        ASSERT_EQ((uint64_t)0, tree.CalculateIdxInSlice(4096));
        ASSERT_EQ((uint64_t)(4097%128), tree.CalculateIdxInSlice(4097));
        ASSERT_EQ((uint64_t)(10000%128), tree.CalculateIdxInSlice(10000));
    }
}

void RadixTreeTest::TestAppendSliceAndSearch()
{
    {
        RadixTree tree(4, 2, mByteSlicePool);
        char slice[] = {'1', '2'};
        //test search nonexist result
        //ASSERT_TRUE(NULL == tree.Search(1000));

        //height = 0
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        //ASSERT_TRUE(NULL == tree.Search(1000));

        //height = 1
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);

        //height = 2
        slice[0] = '3';
        slice[1] = '4';
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
    }

    {
        RadixTree tree(4, 2, mByteSlicePool, 2);
        char slice[] = {'1', '2'};

        //test search nonexist result
        //ASSERT_TRUE(NULL == tree.Search(1000));

        //height = 0
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        //ASSERT_TRUE(NULL == tree.Search(1000));

        //height = 1
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);

        //height = 2
        slice[0] = '3';
        slice[1] = '4';
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
        InnerTestAppendSliceAndSearch(tree, 2, (Slot)slice);
    }
}

void RadixTreeTest::TestAppend()
{
    {
        RadixTree tree(4, 4, mByteSlicePool);
        //test append zero
        ASSERT_TRUE(tree.Append((uint8_t*)"1", 0));
        ASSERT_EQ((uint64_t)0, tree.GetCurrentOffset());
        ASSERT_EQ((uint64_t)1, tree.GetSliceNum());
    }

    {
        RadixTree tree(4, 4, mByteSlicePool);
        const char* appendData = "123456789";
        //test normal case
        InnerTestAppend(tree, appendData, 3, 0);
        //slice left space useless
        InnerTestAppend(tree, appendData, 4, 4);
        //test append data more than one slice
        InnerTestAppend(tree, appendData, 9, 8);
        //continue append small data
        InnerTestAppend(tree, appendData, 2, 17);
        //continue append large data
        InnerTestAppend(tree, appendData, 9, 20);
        InnerTestAppend(tree, appendData, 9, 32);
    }
}

void RadixTreeTest::TestOccupyItem()
{
    RadixTree tree(2, 2, mByteSlicePool, 2);
    ASSERT_EQ((uint64_t)0, tree.GetCurrentOffset());
    tree.OccupyOneItem();
    ASSERT_EQ((uint64_t)2, tree.GetCurrentOffset());
}

void RadixTreeTest::TestGetSliceSize()
{
    RadixTree tree(2, 2, mByteSlicePool, 2);
    ASSERT_EQ((uint64_t)0, tree.GetSliceSize(0));
    tree.OccupyOneItem();
    ASSERT_EQ((uint64_t)2, tree.GetSliceSize(0));
    tree.OccupyOneItem();
    ASSERT_EQ((uint64_t)4, tree.GetSliceSize(0));
    tree.OccupyOneItem();
    ASSERT_EQ((uint64_t)2, tree.GetSliceSize(1));
}

void RadixTreeTest::InnerTestAppend(RadixTree& tree, const char* value,
                                    size_t size, uint64_t expectOffset)
{
    const uint8_t* data = (const uint8_t*)value;
    ASSERT_TRUE(tree.Append(data, size));
    uint64_t offset = tree.GetCurrentOffset() - size;
    ASSERT_EQ(expectOffset, offset);
    uint8_t* expectData = tree.Search(offset);
    for (size_t i = 0; i < size; i++)
    {
        ASSERT_EQ(expectData[i], data[i]);
    }
}

void RadixTreeTest::InnerTestAppendSliceAndSearch(
        RadixTree& tree, uint64_t sliceSize, Slot slot)
{
    tree.AppendSlice(slot);
    Slot expectSlot = (void*)tree.Search(sliceSize * (tree.GetSliceNum() - 1));

    ASSERT_EQ(expectSlot, slot);
    for (size_t i = 0; i < sliceSize; i++)
    {
        ASSERT_EQ(((char*)expectSlot)[i], ((char*)slot)[i]);
    }
}

IE_NAMESPACE_END(common);

