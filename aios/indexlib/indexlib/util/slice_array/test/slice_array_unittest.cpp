#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

#define TEST_SLICE_ARRAY
#include "indexlib/util/slice_array/slice_array.h"
#include <map>

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);

class SliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SliceArrayTest);
    void CaseSetUp() override
    {
        mPool = NULL;
    }

    void CaseTearDown() override
    {
    }

    void PreparePool()
    {    
        mPool = new Pool(256 * 1024);
    }

    void CleanPool()
    {
        if (mPool)
        {
            delete mPool;
            mPool = NULL;
        }
    }
    
    void TestCaseForConstructor()
    {
        InternalTestForConstructor(false, false);
        InternalTestForConstructor(true, false);
        InternalTestForConstructor(true, true);
    }

    void InternalTestForConstructor(bool hasPool, bool isOwner)
    {
        uint32_t sliceLen[] = {1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 11, 12, 13, 14, 15, 16};

        uint32_t i = 0;
        for (; i < sizeof(sliceLen) / sizeof(uint32_t); ++i)
        {
            if (hasPool)
            {
                PreparePool();
            }
            {
                SliceArray<int32_t> array(sliceLen[i], 1, NULL, mPool, isOwner);
                INDEXLIB_TEST_EQUAL(sliceLen[i], array.GetSliceLen());
            }
            if (!isOwner)
            {
                CleanPool();
            }
            
        }
    }
    void TestCaseForIntSliceArraySetGet()
    {
        InternalTestCaseForIntSliceArraySetGet(false, false);
        InternalTestCaseForIntSliceArraySetGet(true, false);
        InternalTestCaseForIntSliceArraySetGet(true, true);
    }

    void InternalTestCaseForIntSliceArraySetGet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 10;
        const uint32_t SLICENUM = 10;
        uint32_t i, j;
        
        if (hasPool)
        {
            PreparePool();
        }
        else 
        {
            mPool = NULL;
        }
        {
            UInt32SliceArray array(SLICELEN, 1, NULL, mPool, isOwner);
        
            for (i = 0; i < SLICENUM; ++i)
            {
                for (j = 0; j < SLICELEN; ++j)
                {
                    array.Set(i * SLICELEN + j, i * SLICELEN + j);

                    INDEXLIB_TEST_EQUAL(array[i * SLICELEN + j], i * SLICELEN + j);
                    INDEXLIB_TEST_EQUAL(array.GetSliceNum(), i + 1);
                    INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), (int32_t)(i * SLICELEN + j));
                    INDEXLIB_TEST_EQUAL(array.GetLastSliceDataLen(), j + 1);

                    if (j < 2 || j > SLICELEN - 2)
                    {
                        for (uint32_t k = 0; k <= i; ++k)
                        {
                            if (k < i)
                            {
                                INDEXLIB_TEST_EQUAL(array.GetSliceDataLen(k), SLICELEN);
                            }
                            else
                            {
                                INDEXLIB_TEST_EQUAL(array.GetSliceDataLen(k), j + 1);
                            }
                        }
                    }

                    if (j % SLICENUM == i)
                    {
                        for (uint32_t k = 0; k < i; ++k)
                        {
                            INDEXLIB_TEST_EQUAL(SLICELEN, array.GetSliceDataLen(k));
                        }
                        INDEXLIB_TEST_EQUAL(j + 1, array.GetSliceDataLen(i));
                    }
                }
            }
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

    void TestCaseForIntSliceArrayDelaySet()
    {
        InternalTestCaseForIntSliceArrayDelaySet(false, false);
        InternalTestCaseForIntSliceArrayDelaySet(true, false);
        InternalTestCaseForIntSliceArrayDelaySet(true, true);
    }

    void InternalTestCaseForIntSliceArrayDelaySet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 10;
        const uint32_t SLICENUM = 10;
        uint32_t i,j;

        if (hasPool)
        {
            PreparePool();
        }
        else
        {
            mPool = NULL;
        }
        {
            UInt32SliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

            for (i = 0; i < SLICENUM; ++i)
            {
                uint32_t* slice = array.GetSlice(i);
                for (j = 0; j < SLICELEN; ++j)
                {
                    slice[j] = i * SLICELEN + j;
                }
                array.SetMaxValidIndex((i + 1) * SLICELEN - 1);

                for (j = 0; j < SLICELEN; ++j)
                {
                    INDEXLIB_TEST_EQUAL(array[i * SLICELEN + j], i * SLICELEN + j);
                    INDEXLIB_TEST_EQUAL(array.GetSliceNum(), i + 1);
                    INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), (int32_t)((i + 1) * SLICELEN - 1));

                    if (j % SLICENUM == i)
                    {
                        for (uint32_t k = 0; k <= i; ++k)
                        {
                            INDEXLIB_TEST_EQUAL(SLICELEN, array.GetSliceDataLen(k));
                        }
                    }
                }
            }
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

    void TestCaseForIntReplaceSlice()
    {
        InternalTestCaseForIntReplaceSlice(false, false);
        InternalTestCaseForIntReplaceSlice(true, false);
        InternalTestCaseForIntReplaceSlice(true, true);
    }

    void InternalTestCaseForIntReplaceSlice(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1021;
        const uint32_t SLICENUM = 2;
        uint32_t i, j;
        if (hasPool)
        {
            PreparePool();
        }
        else
        {
            mPool = NULL;
        }
        {
            UInt32SliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

            for (i = 0; i < SLICENUM; ++i)
            {
                for (j = 0; j < SLICELEN; ++j)
                {
                    if (i == SLICENUM - 1)
                    {
                        if (j < SLICELEN - 2)
                        {
                            array.Set(i * SLICELEN + j, i * SLICELEN + j);
                        }
                    }
                    else
                    {
                        array.Set(i * SLICELEN + j, i * SLICELEN + j);
                    }
                }
            }
            INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), SLICENUM * SLICELEN - 3);

            uint32_t* newSlice = new uint32_t[SLICELEN];
            for (i = 0; i < SLICELEN; ++i)
            {
                newSlice[i] = i + 987843;
            }

            INDEXLIB_EXPECT_EXCEPTION(array.ReplaceSlice(SLICENUM, newSlice), InconsistentStateException);
            array.ReplaceSlice(SLICENUM - 1, newSlice);

            INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), SLICENUM * SLICELEN - 1);

            for (i = 0; i < SLICENUM; ++i)
            {
                for (j = 0; j < SLICELEN; ++j)
                {
                    if (i == SLICENUM - 1)
                    {
                        INDEXLIB_TEST_EQUAL(j + 987843, array[i * SLICELEN + j]);
                    }
                    else
                    {
                        INDEXLIB_TEST_EQUAL(i * SLICELEN + j, array[i * SLICELEN + j]);
                    }
                }
            }
            delete[] newSlice;
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

    void TestCaseForGetSliceAndZeroTail()
    {
        InternalTestCaseForGetSliceAndZeroTail(false, false);
        InternalTestCaseForGetSliceAndZeroTail(true, false);
        InternalTestCaseForGetSliceAndZeroTail(true, true);
    }

    void InternalTestCaseForGetSliceAndZeroTail(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
        const uint32_t SLICENUM = 128;
        const uint32_t TAIL_SIZE = 10;
        if (hasPool)
        {
            PreparePool();
        }
        else 
        {
            mPool = NULL;
        }
        {
            UInt32SliceArray array(SLICELEN, 1, NULL, mPool, isOwner);
            for (uint32_t i = 0; i < SLICELEN * SLICENUM - TAIL_SIZE; ++i)
            {
                array.Set((int32_t)i, i);
            }
            const uint32_t* tailSliceData = array.GetSliceAndZeroTail(SLICENUM - 1);

            uint32_t j;
            for (j = 0; j < SLICELEN * SLICENUM - TAIL_SIZE; ++j)
            {
                INDEXLIB_TEST_EQUAL(j, array[j]);
            }

            for (j = SLICELEN - TAIL_SIZE; j < SLICELEN; ++j)
            {
                INDEXLIB_TEST_EQUAL((uint32_t)0, tailSliceData[j]);
            }
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

private:
    Pool *mPool;
};

INDEXLIB_UNIT_TEST_CASE(SliceArrayTest, TestCaseForConstructor);
INDEXLIB_UNIT_TEST_CASE(SliceArrayTest, TestCaseForIntSliceArraySetGet);
INDEXLIB_UNIT_TEST_CASE(SliceArrayTest, TestCaseForIntSliceArrayDelaySet);
INDEXLIB_UNIT_TEST_CASE(SliceArrayTest, TestCaseForIntReplaceSlice);
INDEXLIB_UNIT_TEST_CASE(SliceArrayTest, TestCaseForGetSliceAndZeroTail);

IE_NAMESPACE_END(util);
