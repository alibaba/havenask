#include "indexlib/util/byte_slice_list/ByteSliceListIterator.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {

class ByteSliceListIteratorTest : public INDEXLIB_TESTBASE
{
public:
    ByteSliceListIteratorTest();
    ~ByteSliceListIteratorTest();

    DECLARE_CLASS_NAME(ByteSliceListIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSeekSlice();
    void TestNext();

private:
    ByteSlice* CreateSlice(uint32_t itemCount);

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestSeekSlice);
INDEXLIB_UNIT_TEST_CASE(ByteSliceListIteratorTest, TestNext);
AUTIL_LOG_SETUP(indexlib.util, ByteSliceListIteratorTest);

ByteSliceListIteratorTest::ByteSliceListIteratorTest() {}

ByteSliceListIteratorTest::~ByteSliceListIteratorTest() {}

void ByteSliceListIteratorTest::CaseSetUp() {}

void ByteSliceListIteratorTest::CaseTearDown() {}

void ByteSliceListIteratorTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");

    ByteSlice* slice = NULL;
    ByteSliceList sliceList;
    slice = CreateSlice(5);
    sliceList.Add(slice);

    ByteSliceListIterator iter(&sliceList);

    iter.SeekSlice(1);
    void* data = NULL;
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

    ByteSlice* slice1 = CreateSlice(5);
    ByteSlice* slice2 = CreateSlice(10);
    ByteSlice* slice3 = CreateSlice(15);
    ByteSliceList sliceList;
    sliceList.Add(slice1);
    sliceList.Add(slice2);
    sliceList.Add(slice3);

    {
        // SeekSlice to same position
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);
    }
    {
        // SeekSlice back
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);

        ASSERT_FALSE(iter.SeekSlice(2));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);
    }
    {
        // SeekSlice in same slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(2));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)2, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(4));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)4, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);
    }
    {
        // SeekSlice to next slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(3));
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(6));
        ASSERT_EQ(slice2, iter._slice);
        ASSERT_EQ((size_t)1, iter._posInSlice);
        ASSERT_EQ((size_t)5, iter._seekedSliceSize);
    }
    {
        // SeekSlice to border of slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_TRUE(iter.SeekSlice(14));
        ASSERT_EQ(slice2, iter._slice);
        ASSERT_EQ((size_t)9, iter._posInSlice);
        ASSERT_EQ((size_t)5, iter._seekedSliceSize);

        ASSERT_TRUE(iter.SeekSlice(15));
        ASSERT_EQ(slice3, iter._slice);
        ASSERT_EQ((size_t)0, iter._posInSlice);
        ASSERT_EQ((size_t)15, iter._seekedSliceSize);
    }
    {
        // SeekSlice cross the border of last slice
        ByteSliceListIterator iter(&sliceList);
        ASSERT_FALSE(iter.SeekSlice(30));
        ASSERT_EQ(NULL, iter._slice);
        ASSERT_EQ((size_t)0, iter._posInSlice);
        ASSERT_EQ((size_t)30, iter._seekedSliceSize);
    }
}

void ByteSliceListIteratorTest::TestNext()
{
    SCOPED_TRACE("Failed");

    ByteSlice* slice1 = CreateSlice(5);
    ByteSlice* slice2 = CreateSlice(10);
    ByteSlice* slice3 = CreateSlice(15);
    ByteSliceList sliceList;
    sliceList.Add(slice1);
    sliceList.Add(slice2);
    sliceList.Add(slice3);
    void* data = NULL;
    size_t size = 0;

    {
        ByteSliceListIterator iter(&sliceList);
        ASSERT_FALSE(iter.HasNext(0));

        ASSERT_TRUE(iter.HasNext(3));
        iter.Next(data, size);
        ASSERT_EQ(slice1, iter._slice);
        ASSERT_EQ((size_t)3, iter._posInSlice);
        ASSERT_EQ((size_t)0, iter._seekedSliceSize);
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

        for (size_t i = 0; i < size; ++i) {
            ASSERT_EQ((char)(i + 3), ((char*)data)[i]);
        }
        // second slice
        ASSERT_TRUE(iter.HasNext(17));
        iter.Next(data, size);
        ASSERT_EQ((size_t)10, size);

        for (size_t i = 0; i < size; ++i) {
            ASSERT_EQ((char)i, ((char*)data)[i]);
        }
        // third slice
        ASSERT_TRUE(iter.HasNext(17));
        iter.Next(data, size);
        ASSERT_EQ((size_t)2, size);

        for (size_t i = 0; i < size; ++i) {
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
    for (size_t i = 0; i < itemCount; i++) {
        ((char*)byteSlice->data)[i] = (char)i;
    }
    return byteSlice;
}
}} // namespace indexlib::util
