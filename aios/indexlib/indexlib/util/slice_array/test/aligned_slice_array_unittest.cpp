#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

#define TEST_ALIGNED_SLICE_ARRAY
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include <map>
using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);

#pragma pack(1)
typedef struct _SData
{
    int32_t f1;
    int16_t f2;
    int8_t  f3;
}SData;
#pragma pack()

class AlignedSliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AlignedSliceArrayTest);
    void CaseSetUp() override
    {
        mPool = NULL;
    }

    void CaseTearDown() override
    {
    }

    void PreparePool()
    {
        mPool = new autil::mem_pool::Pool(256 * 1024);
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
        InternalTestCaseForConstructor(false, false);
        InternalTestCaseForConstructor(true, false);
        InternalTestCaseForConstructor(true, true);
    }

    void InternalTestCaseForConstructor(bool hasPool, bool isOwner)
    {
        uint32_t sliceLen[] =     {1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 11, 12, 13, 14, 15, 16};
        uint32_t realSliceLen[] = {1, 2, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 16, 16, 16, 16};
        uint32_t powerOf2[] =       {0, 1, 2, 2, 3, 3, 3, 3,  4,  4,  4,  4,  4,  4,  4,  4};
        
        uint32_t i = 0;
        for (; i < sizeof(sliceLen) / sizeof(uint32_t); ++i)
        {
            if (hasPool)
            {
                PreparePool();
            }
            {
                Int32AlignedSliceArray array(sliceLen[i], 1, NULL, mPool, isOwner);
                INDEXLIB_TEST_EQUAL(realSliceLen[i], array.GetSliceLen());
                INDEXLIB_TEST_EQUAL(powerOf2[i], array.mPowerOf2);
            }
            if (!isOwner)
            {
                CleanPool();
            }
        }
    }

    void TestCaseForIntAlignedSliceArraySetGet()
    {
        InternalTestCaseForIntAlignedSliceArraySetGet(false, false);
        InternalTestCaseForIntAlignedSliceArraySetGet(true, false);
        InternalTestCaseForIntAlignedSliceArraySetGet(true, true);
    }

    void InternalTestCaseForIntAlignedSliceArraySetGet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
        const uint32_t SLICENUM = 128;
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
            UInt32AlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

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

    void TestCaseForIntAlignedSliceArrayDelaySet()
    {
        InternalTestCaseForIntAlignedSliceArrayDelaySet(false, false);
        InternalTestCaseForIntAlignedSliceArrayDelaySet(true, false);
        InternalTestCaseForIntAlignedSliceArrayDelaySet(true, true);
    }

    void InternalTestCaseForIntAlignedSliceArrayDelaySet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
        const uint32_t SLICENUM = 128;
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
            UInt32AlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

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
        const uint32_t SLICELEN = 1024;
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
            UInt32AlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

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
    
    void TestCaseForByteAlignedSliceArraySetGet()
    {
        InternalTestCaseForByteAlignedSliceArraySetGet(false, false);
        InternalTestCaseForByteAlignedSliceArraySetGet(true, false);
        InternalTestCaseForByteAlignedSliceArraySetGet(true, true);
    }

    void InternalTestCaseForByteAlignedSliceArraySetGet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
        const uint32_t DATANUM  = 20000;

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
            ByteAlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

            SData    wdata;
            SData    rdata;
            int32_t  seeds[] = {7361985, 32111, 119};
            uint32_t dataSize = sizeof(SData);

            j = 0;
            for (i = 0; i < DATANUM; ++i)
            {
                wdata.f1 = (int32_t)((i * DATANUM + j++) % seeds[0]);
                wdata.f2 = (int16_t)((i * DATANUM + j++) % seeds[1]);
                wdata.f3 = (int8_t) ((i * DATANUM + j++) % seeds[2]);
                array.SetTypedValue<SData>(i * dataSize, wdata);
                array.GetTypedValue<SData>(i * dataSize, rdata);
                INDEXLIB_TEST_EQUAL(wdata.f1, rdata.f1);
                INDEXLIB_TEST_EQUAL(wdata.f2, rdata.f2);
                INDEXLIB_TEST_EQUAL(wdata.f3, rdata.f3);
                INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), (int32_t)((i + 1)* dataSize - 1));

                uint32_t lastSliceLen = (i + 1) * dataSize % SLICELEN;
                if (lastSliceLen == 0) { lastSliceLen = SLICELEN; }
                INDEXLIB_TEST_EQUAL(array.GetLastSliceDataLen(), lastSliceLen);

                uint32_t sliceNum = 0;
                if ((i + 1) * dataSize % SLICELEN == 0)
                {
                    sliceNum = (i + 1) * dataSize / SLICELEN;
                }
                else
                {
                    sliceNum = (i + 1) * dataSize / SLICELEN + 1;
                }
                INDEXLIB_TEST_EQUAL(array.GetSliceNum(), sliceNum);
            }

            uint32_t sliceNum = array.GetSliceNum();
            for (j = 0; j < sliceNum; ++j)
            {
                if (j == sliceNum - 1)
                {
                    uint32_t sliceLen = i * dataSize % SLICELEN;
                    if (sliceLen == 0)
                    {
                        sliceLen = SLICELEN;
                    }
                    INDEXLIB_TEST_EQUAL(array.GetSliceDataLen(j), i * dataSize % SLICELEN);
                }
                else 
                {
                    INDEXLIB_TEST_EQUAL(array.GetSliceDataLen(j), SLICELEN);
                }
            }
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

    void TestCaseForByteAlignedSliceArrayDelayGet()
    {
        InternalTestCaseForByteAlignedSliceArrayDelaySet(false, false);
        InternalTestCaseForByteAlignedSliceArrayDelaySet(true, false);
        InternalTestCaseForByteAlignedSliceArrayDelaySet(true, true);
    }

    void InternalTestCaseForByteAlignedSliceArrayDelaySet(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
        const uint32_t SLICENUM = 128;

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
            ByteAlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

            SData  * pdata = NULL;
            char   * pRemainByte = NULL;
            SData    rdata;
            char     remainByte;    

            int32_t  seeds[] = {7361985, 32111, 119};
            uint32_t dataSize = sizeof(SData);

            uint32_t sliceDataNum = SLICELEN / dataSize;
            uint32_t remainByteNum = SLICELEN % dataSize;
            uint32_t remainByteOffset = SLICELEN - remainByteNum;

            for (i = 0; i < SLICENUM; ++i)
            {
                pdata = (SData *)array.GetSlice(i);
                for (j = 0; j < sliceDataNum; ++j)
                {
                    pdata->f1 = (int32_t)((i * SLICELEN + j) % seeds[0]);
                    pdata->f2 = (int16_t)((i * SLICELEN + j) % seeds[1]);
                    pdata->f3 = (int8_t) ((i * SLICELEN + j) % seeds[2]);
                    pdata++;
                }

                pRemainByte = (char *)pdata;
                for (j = 0; j < remainByteNum; ++j)
                {
                    *pRemainByte = (char)((i + j) % 127);
                    pRemainByte++;
                }

                array.SetMaxValidIndex((i + 1) * SLICELEN - 1);

                for (j = 0; j < sliceDataNum; ++j)
                {
                    array.GetTypedValue<SData>(i * SLICELEN + j * dataSize, rdata);
                    INDEXLIB_TEST_EQUAL((int32_t)((i * SLICELEN + j) % seeds[0]), rdata.f1);
                    INDEXLIB_TEST_EQUAL((int16_t)((i * SLICELEN + j) % seeds[1]), rdata.f2);
                    INDEXLIB_TEST_EQUAL((int8_t) ((i * SLICELEN + j) % seeds[2]), rdata.f3);
                }

                for (j = 0; j < remainByteNum; ++j)
                {
                    array.GetTypedValue<char>(i * SLICELEN + remainByteOffset + j, remainByte);
                    INDEXLIB_TEST_EQUAL((char)((i + j) % 127), remainByte);
                }
                INDEXLIB_TEST_EQUAL(array.GetSliceNum(), i + 1);
                INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), (int32_t)((i + 1) * SLICELEN - 1));
            }
        }
        if (!isOwner)
        {
            CleanPool();
        }
    }

    void TestCaseForByteReplaceSlice()
    {
        InternalTestCaseForByteReplaceSlice(false, false);
        InternalTestCaseForByteReplaceSlice(true, false);
        InternalTestCaseForByteReplaceSlice(true, true);
    }

    void InternalTestCaseForByteReplaceSlice(bool hasPool, bool isOwner)
    {
        const uint32_t SLICELEN = 1024;
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
            ByteAlignedSliceArray array(SLICELEN, 1, NULL, mPool, isOwner);

            for (i = 0; i < SLICENUM; ++i)
            {
                for (j = 0; j < SLICELEN; ++j)
                {
                    if (i == SLICENUM - 1)
                    {
                        if (j < SLICELEN - 2)
                        {
                            array.Set(i * SLICELEN + j, (i * SLICELEN + j) % 128);
                        }
                    }
                    else
                    {
                        array.Set(i * SLICELEN + j, (i * SLICELEN + j) % 128);
                    }
                }
            }
            INDEXLIB_TEST_EQUAL(array.GetMaxValidIndex(), SLICENUM * SLICELEN - 3);

            char * newSlice = new char[SLICELEN];
            for (i = 0; i < SLICELEN; ++i)
            {
                newSlice[i] = (i + 987843) % 128;
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
                        INDEXLIB_TEST_EQUAL((j + 987843) % 128, (uint32_t)array[i * SLICELEN + j]);
                    }
                    else
                    {
                        INDEXLIB_TEST_EQUAL((i * SLICELEN + j) % 128, (uint32_t)array[i * SLICELEN + j]);
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
    
    void TestCaseForGetLastSliceDataLen()
    {
        const uint32_t SLICELEN = 1024;        
        ByteAlignedSliceArray array(SLICELEN, 1, NULL, mPool, false);
        ASSERT_EQ((uint32_t)0, array.GetSliceNum());
        ASSERT_EQ((uint32_t)0, array.GetSliceDataLen(0));

        int64_t index = 0;
        char temp[SLICELEN] = {0};
        array.SetList(0, temp, SLICELEN);
        ASSERT_EQ((uint32_t)1, array.GetSliceNum());
        ASSERT_EQ((uint32_t)SLICELEN, array.GetSliceDataLen(0));
        index += SLICELEN;

        array.SetList(index, temp, 0);
        ASSERT_EQ((uint32_t)1, array.GetSliceNum());
        ASSERT_EQ((uint32_t)SLICELEN, array.GetSliceDataLen(0));
        ASSERT_EQ((uint32_t)0, array.GetSliceDataLen(1));
    }


private:
    Pool *mPool;
};

INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForConstructor);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForIntAlignedSliceArraySetGet);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForIntAlignedSliceArrayDelaySet);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForIntReplaceSlice);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForByteAlignedSliceArraySetGet);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForByteAlignedSliceArrayDelayGet);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForByteReplaceSlice);
INDEXLIB_UNIT_TEST_CASE(AlignedSliceArrayTest, TestCaseForGetLastSliceDataLen);

IE_NAMESPACE_END(util);
