#ifndef __INDEXLIB_POSTINGDECODERIMPLTEST_H
#define __INDEXLIB_POSTINGDECODERIMPLTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/common/numeric_compress/int_encoder.h"

IE_NAMESPACE_BEGIN(index);

class PostingDecoderImplTest : public INDEXLIB_TESTBASE {
public:
    class MockInt32Encoder : public common::Int32Encoder
    {
    public:
        MOCK_CONST_METHOD3(Encode, uint32_t(common::ByteSliceWriter&, const uint32_t*, uint32_t));
        MOCK_CONST_METHOD3(Encode, uint32_t(uint8_t*, const uint32_t*, uint32_t));
        MOCK_CONST_METHOD3(Decode, uint32_t(uint32_t*, uint32_t, common::ByteSliceReader&));
    };

    class MockInt16Encoder : public common::Int16Encoder
    {
    public:
        MOCK_CONST_METHOD3(Encode, uint32_t(common::ByteSliceWriter&, const uint16_t*, uint32_t));
        MOCK_CONST_METHOD3(Decode, uint32_t(uint16_t*, uint32_t, common::ByteSliceReader&));
    };

    class MockInt8Encoder : public common::Int8Encoder
    {
    public:
        MOCK_CONST_METHOD3(Encode, uint32_t(common::ByteSliceWriter&, const uint8_t*, uint32_t));
        MOCK_CONST_METHOD3(Decode, uint32_t(uint8_t*, uint32_t, common::ByteSliceReader&));
    };

public:
    PostingDecoderImplTest();
    ~PostingDecoderImplTest();
public:
    void SetUp();
    void TearDown();
    void TestInit();
    void TestInitForDictInlineCompress();
    void TestDecodeDictInlineDocList();
    void TestMissPositionBitmap();
    void TestDecodeDocList();
    void TestDocListCollapsed();
    void TestDecodePosList();
    void TestPosListCollapsed();

public:
    void InnerTest(optionflag_t optionFlag,
                   df_t df, bool hasBitmap = true);

    void InnerTestDecodeDocList(optionflag_t optionFlag, uint32_t docNum, 
                                bool collapse = false);

    void InnerTestDecodePosList(optionflag_t optionFlag, uint32_t posNum, 
                                bool collapse = false);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestInitForDictInlineCompress);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestMissPositionBitmap);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestDecodeDocList);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestDocListCollapsed);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestDecodePosList);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestPosListCollapsed);
INDEXLIB_UNIT_TEST_CASE(PostingDecoderImplTest, TestDecodeDictInlineDocList);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGDECODERIMPLTEST_H
