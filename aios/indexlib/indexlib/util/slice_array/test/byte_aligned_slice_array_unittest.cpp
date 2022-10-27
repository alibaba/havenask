#include "indexlib/util/slice_array/test/byte_aligned_slice_array_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ByteAlignedSliceArrayTest);

ByteAlignedSliceArrayTest::ByteAlignedSliceArrayTest()
{
}

ByteAlignedSliceArrayTest::~ByteAlignedSliceArrayTest()
{
}

void ByteAlignedSliceArrayTest::SetUp()
{
}

void ByteAlignedSliceArrayTest::TearDown()
{
}

void ByteAlignedSliceArrayTest::TestSetByteList()
{
    ByteAlignedSliceArray sliceArray(128);
    char buffer[128];

    sliceArray.SetByteList(0, 'A', 128);
    sliceArray.Read(0, buffer, 128);
    for (size_t i = 0; i < 128; ++i)
    {
        ASSERT_EQ('A', buffer[i]);
    }

    sliceArray.SetByteList(0, 'C', 128);
    sliceArray.Read(0, buffer, 128);
    for (size_t i = 0; i < 128; ++i)
    {
        ASSERT_EQ('C', buffer[i]);
    }
}

void ByteAlignedSliceArrayTest::TestGetDataLen()
{
    const uint32_t SLICELEN = 1024;        
    ByteAlignedSliceArray array(SLICELEN);
    ASSERT_EQ((uint32_t)0, array.GetSliceNum());
    ASSERT_EQ((uint32_t)0, array.GetSliceDataLen(0));
    
    int64_t index = 0;
    char temp[SLICELEN] = {0};
    array.SetList(0, temp, SLICELEN);
    ASSERT_EQ((uint32_t)1, array.GetSliceNum());
    ASSERT_EQ((uint32_t)SLICELEN, array.GetSliceDataLen(0));
    index += SLICELEN;
    
    array.SetByteList(index, 'C', 0);
    ASSERT_EQ((uint32_t)1, array.GetSliceNum());
    ASSERT_EQ((uint32_t)SLICELEN, array.GetSliceDataLen(0));
    ASSERT_EQ((uint32_t)0, array.GetSliceDataLen(1));
}

IE_NAMESPACE_END(util);

