#include "indexlib/util/byte_slice_list/test/byte_slice_list_iterator_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ByteSliceListIteratorTest);

ByteSliceListIteratorTest::ByteSliceListIteratorTest()
{
}

ByteSliceListIteratorTest::~ByteSliceListIteratorTest()
{
}

void ByteSliceListIteratorTest::CaseSetUp()
{
}

void ByteSliceListIteratorTest::CaseTearDown()
{
}

void ByteSliceListIteratorTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");

    ByteSlice * slice = NULL;
    ByteSliceList sliceList;
    slice = CreateSlice(5);
    sliceList.Add(slice);

    ByteSliceListIterator iter(&sliceList);

    iter.SeekSlice(1);
    void *data = NULL;
    size_t size = 0;

    ASSERT_TRUE(iter.HasNext(3));
    iter.Next(data, size);
    ASSERT_EQ((size_t)2, size);
    ASSERT_EQ((char)1, ((char*)data)[0]);
    ASSERT_EQ((char)2, ((char*)data)[1]);
}

void ByteSliceListIteratorTest::TestSeekSlice()
{
    SCOPED_TRACE("Failed");

    ByteSlice *slice1 = CreateSlice(5);
    ByteSlice *slice2 = CreateSlice(10);
    ByteSlice *slice3 = CreateSlice(15);
    ByteSliceList sliceList;
    sliceList.Add(slice1);
    sliceList.Add(slice2);
    sliceList.Add(slice3);

    {
        // SeekSlice to same position
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);
    }    
    {
        // SeekSlice back
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);

        ASSERT_FALSE(iter.SeekSlice(2));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);
    }
    {
        // SeekSlice in same slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(2));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)2, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(4));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)4, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);
    }
    {
        // SeekSlice to next slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(6));
        ASSERT_EQ(slice2, iter.mSlice);
        ASSERT_EQ((size_t)1, iter.mPosInSlice);
        ASSERT_EQ((size_t)5, iter.mSeekedSliceSize);
    }
    {
        // SeekSlice to border of slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(14));
        ASSERT_EQ(slice2, iter.mSlice);
        ASSERT_EQ((size_t)9, iter.mPosInSlice);
        ASSERT_EQ((size_t)5, iter.mSeekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(15));
        ASSERT_EQ(slice3, iter.mSlice);
        ASSERT_EQ((size_t)0, iter.mPosInSlice);
        ASSERT_EQ((size_t)15, iter.mSeekedSliceSize);
    }
    {
        // SeekSlice cross the border of last slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_FALSE(iter.SeekSlice(30));
        ASSERT_EQ(NULL, iter.mSlice);
        ASSERT_EQ((size_t)0, iter.mPosInSlice);
        ASSERT_EQ((size_t)30, iter.mSeekedSliceSize);
    }
}


void ByteSliceListIteratorTest::TestNext()
{
    SCOPED_TRACE("Failed");

    ByteSlice *slice1 = CreateSlice(5);
    ByteSlice *slice2 = CreateSlice(10);
    ByteSlice *slice3 = CreateSlice(15);
    ByteSliceList sliceList;
    sliceList.Add(slice1);
    sliceList.Add(slice2);
    sliceList.Add(slice3);
    void *data = NULL;
    size_t size = 0;

    {
        ByteSliceListIterator iter(&sliceList);
        ASSERT_FALSE(iter.HasNext(0));

        ASSERT_TRUE(iter.HasNext(3));
        iter.Next(data, size);
        ASSERT_EQ(slice1, iter.mSlice);
        ASSERT_EQ((size_t)3, iter.mPosInSlice);
        ASSERT_EQ((size_t)0, iter.mSeekedSliceSize);
        ASSERT_EQ((size_t)3, size);
        ASSERT_EQ((char)0, ((char*)data)[0]);
        ASSERT_EQ((char)1, ((char*)data)[1]);
        ASSERT_EQ((char)2, ((char*)data)[2]);
    }
    {
        ByteSliceListIterator iter(&sliceList);

        iter.SeekSlice(3);

        // first slice
        ASSERT_TRUE(iter.HasNext(17));
        iter.Next(data, size);
        ASSERT_EQ((size_t)2, size);

        for (size_t i = 0; i < size; ++i)
        {
            ASSERT_EQ((char)(i + 3), ((char*)data)[i]);
        }
        // second slice
        ASSERT_TRUE(iter.HasNext(17));
        iter.Next(data, size);
        ASSERT_EQ((size_t)10, size);

        for (size_t i = 0; i < size; ++i)
        {
            ASSERT_EQ((char)i, ((char*)data)[i]);
        }
        // third slice
        ASSERT_TRUE(iter.HasNext(17));
        iter.Next(data, size);
        ASSERT_EQ((size_t)2, size);

        for (size_t i = 0; i < size; ++i)
        {
            ASSERT_EQ((char)i, ((char*)data)[i]);
        }
        ASSERT_FALSE(iter.HasNext(17));
    }
    {
        ByteSliceListIterator iter(&sliceList);

        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_FALSE(iter.HasNext(31));
    }
    
}

ByteSlice* ByteSliceListIteratorTest::CreateSlice(uint32_t itemCount)
{
    ByteSlice* byteSlice = ByteSlice::CreateObject(itemCount);
    for (size_t i = 0; i < itemCount; i++)
    {
        ((char*)byteSlice->data)[i] = (char)i;
    }
    return byteSlice;
}

IE_NAMESPACE_END(util);

