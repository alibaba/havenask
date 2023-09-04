#include "indexlib/index/inverted_index/format/InDocPositionIteratorState.h"

#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class InDocPositionIteratorStateTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

class InDocPositionIteratorStateMock : public InDocPositionIteratorState
{
public:
    InDocPositionIteratorStateMock(uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE,
                                   optionflag_t optionFlag = OPTION_FLAG_ALL)
        : InDocPositionIteratorState(compressMode, optionFlag)
    {
    }
    InDocPositionIteratorStateMock(const InDocPositionIteratorStateMock& other) : InDocPositionIteratorState(other) {}
    InDocPositionIterator* CreateInDocIterator() const override { return nullptr; }
    void Free() override {}
};

TEST_F(InDocPositionIteratorStateTest, testCopy)
{
    InDocPositionIteratorStateMock state(SHORT_LIST_COMPRESS_MODE, of_doc_payload);
    PositionListSegmentDecoder decoder(of_doc_payload, (autil::mem_pool::Pool*)nullptr);
    state.SetTermFreq(12);
    state.SetPositionListSegmentDecoder(&decoder);
    state.SetOffsetInRecord(6);
    state.SetTotalPosCount(11);
    state.SetRecordOffset(10);

    InDocPositionIteratorStateMock copiedState(state);
    ASSERT_EQ(state.GetTermFreq(), copiedState.GetTermFreq());
    ASSERT_EQ(state.GetPositionListSegmentDecoder(), copiedState.GetPositionListSegmentDecoder());
    ASSERT_EQ(state.GetCompressMode(), copiedState.GetCompressMode());
    ASSERT_EQ(state.GetDocId(), copiedState.GetDocId());
    ASSERT_EQ(state.GetTotalPosCount(), copiedState.GetTotalPosCount());
    ASSERT_EQ(state.GetRecordOffset(), copiedState.GetRecordOffset());
}

} // namespace indexlib::index
