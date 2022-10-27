#ifndef __INDEXLIB_INMEMPOSITIONLISTDECODERTEST_H
#define __INDEXLIB_INMEMPOSITIONLISTDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"

IE_NAMESPACE_BEGIN(index);

class InMemPositionListDecoderTest : public INDEXLIB_TESTBASE
{
public:
    class MockSkipListReader : public index::PairValueSkipListReader
    {
    public:
        MOCK_METHOD5(SkipTo, bool(uint32_t queryKey, uint32_t& key, 
                        uint32_t& prevKey, uint32_t& value, uint32_t& delta));

        MOCK_CONST_METHOD0(GetLastValueInBuffer, uint32_t());
    };
public:
    InMemPositionListDecoderTest();
    ~InMemPositionListDecoderTest();

    DECLARE_CLASS_NAME(InMemPositionListDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

    void TestShortListDecodeWithoutPayload();
    void TestShortListDecodeWithPayload();

    void TestDecodeWithoutPayload();
    void TestDecodeWithPayload();
    void TestSkipTo();

    void TestSkipAndLocateAndDecodeWithoutPayload();
    void TestSkipAndLocateAndDecodeWithPayload();

    void TestDestruction();

private:
    void TestDecodeWithOptionFlag(const optionflag_t flag, tf_t tf,
                                  pos_t* posList, pospayload_t* payloadList,
                                  bool needFlush);

    void InnerTestDecode(const optionflag_t flag, tf_t tf);

    void InnerTestSkipAndLocateAndDecode(optionflag_t flag, ttf_t ttf,
            bool needFlush);

private:
    autil::mem_pool::RecyclePool mBufferPool;
    autil::mem_pool::Pool mByteSlicePool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestShortListDecodeWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestShortListDecodeWithPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestDecodeWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestDecodeWithPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestSkipTo);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestSkipAndLocateAndDecodeWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestSkipAndLocateAndDecodeWithPayload);
INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderTest, TestDestruction);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMPOSITIONLISTDECODERTEST_H
