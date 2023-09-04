#include "indexlib/index/test/var_len_data_accessor_unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataAccessorTest);

VarLenDataAccessorTest::VarLenDataAccessorTest() {}

VarLenDataAccessorTest::~VarLenDataAccessorTest() {}

void VarLenDataAccessorTest::CaseSetUp() {}

void VarLenDataAccessorTest::CaseTearDown() {}

void VarLenDataAccessorTest::TestAddEncodedField()
{
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, true);
    VarLenDataAccessor::EncodeMapPtr encodeMap = accessor.mEncodeMap;
    StringView str("1", 1);
    accessor.AppendValue(str);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)1, encodeMap->Size());
    ASSERT_EQ((*(accessor.mOffsets))[0], *(encodeMap->Find(1)));
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[0]);

    accessor.AppendValue(str);
    ASSERT_EQ((size_t)2, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)1, encodeMap->Size());
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[1]);

    str = {"2", 1};
    accessor.AppendValue(str);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((size_t)2, encodeMap->Size());
    ASSERT_EQ((*(accessor.mOffsets))[2], *(encodeMap->Find(2)));
    // sizeof(uint32_t) + datasize
    ASSERT_EQ((uint64_t)5, (uint64_t)(*(accessor.mOffsets))[2]);
}

void VarLenDataAccessorTest::TestAddNormalField()
{
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, false);

    StringView str("1", 1);
    accessor.AppendValue(str);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)0, (uint64_t)(*(accessor.mOffsets))[0]);

    accessor.AppendValue(str);
    ASSERT_EQ((size_t)2, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (uint64_t)(*(accessor.mOffsets))[1]);
}

void VarLenDataAccessorTest::TestReadData()
{
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, false);

    StringView str("1", 1);
    accessor.AppendValue(str);

    uint8_t* data = NULL;
    uint32_t dataLength = 0;
    accessor.ReadData((docid_t)0, data, dataLength);
    ASSERT_EQ((uint32_t)1, dataLength);
    ASSERT_EQ((char)'1', data[0]);

    // test read non exist doc
    accessor.ReadData((docid_t)1, data, dataLength);
    ASSERT_EQ((uint32_t)0, dataLength);
    ASSERT_TRUE(!data);
}

void VarLenDataAccessorTest::TestUpdateNormalField()
{
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, false);
    // docid: 0, value 1
    // docid: 1, value 1
    StringView str("1", 1);
    accessor.AppendValue(str);

    str = {"12", 2};
    accessor.UpdateValue((docid_t)0, str);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (*(accessor.mOffsets))[0]);

    str = {"25", 2};
    accessor.UpdateValue((docid_t)0, str);
    ASSERT_EQ((size_t)1, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)11, (*(accessor.mOffsets))[0]);
}

void VarLenDataAccessorTest::TestUpdateEncodedField()
{
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, true);
    VarLenDataAccessor::EncodeMapPtr encodeMap = accessor.mEncodeMap;
    // docid: 0, value 1
    // docid: 1, value 1
    StringView str("1", 1);
    accessor.AppendValue(str);
    accessor.AppendValue(str);

    // docid: 2, value 2
    // begin offset 5
    str = {"2", 1};
    accessor.AppendValue(str);

    // update: doc2 with uniq value
    str = {"1", 1};
    accessor.UpdateValue((docid_t)2, str);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)0, (*(accessor.mOffsets))[2]);

    // update: doc1 with uniq value
    str = {"2", 1};
    accessor.UpdateValue((docid_t)1, str);
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)5, (*(accessor.mOffsets))[1]);

    // update: doc0 without uniq value
    str = {"3", 1};
    accessor.UpdateValue((docid_t)0, str);
    ASSERT_EQ((size_t)3, encodeMap->Size());
    ASSERT_EQ((uint64_t)10, *(encodeMap->Find(3)));
    ASSERT_EQ((size_t)3, accessor.mOffsets->Size());
    ASSERT_EQ((uint64_t)10, (*(accessor.mOffsets))[0]);

    // update: docid too large
    ASSERT_FALSE(accessor.UpdateValue((docid_t)10000, str));
}

void VarLenDataAccessorTest::TestAppendData()
{
    // normal case
    VarLenDataAccessor accessor;
    accessor.Init(&mPool, false);
    StringView str("1", 1);
    uint64_t offset = accessor.AppendData(str);
    ASSERT_EQ((uint64_t)0, offset);
    uint8_t* buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)1, *(uint32_t*)buffer);
    ASSERT_EQ('1', *((char*)buffer + 4));

    str = {"12", 2};
    offset = accessor.AppendData(str);
    ASSERT_EQ((uint64_t)5, offset);
    buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)2, *(uint32_t*)buffer);
    ASSERT_EQ('1', *((char*)buffer + 4));
    ASSERT_EQ('2', *((char*)buffer + 5));

    // test append large data
    buffer = (uint8_t*)mPool.allocate(1024 * 1024 * 1024);
    str = {(char*)buffer, 1024 * 1024 * 1024};
    offset = accessor.AppendData(str);
    ASSERT_EQ((uint64_t)1024 * 1024, offset);
    buffer = accessor.mData->Search(offset);
    ASSERT_EQ((uint32_t)1024 * 1024 * 1024, *(uint32_t*)buffer);
}
}} // namespace indexlib::index
