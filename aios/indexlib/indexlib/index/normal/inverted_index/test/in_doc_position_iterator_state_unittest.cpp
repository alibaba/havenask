#include "indexlib/index/normal/inverted_index/test/in_doc_position_iterator_state_unittest.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"

using namespace std;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InDocPositionIteratorStateTest);

class InDocPositionIteratorStateMock : public InDocPositionIteratorState
{
public:
    InDocPositionIteratorStateMock(uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE, 
            optionflag_t optionFlag = OPTION_FLAG_ALL)
        : InDocPositionIteratorState(compressMode, optionFlag)
    {}
    InDocPositionIteratorStateMock(const InDocPositionIteratorStateMock& other)
        : InDocPositionIteratorState(other)
    {}
    InDocPositionIterator* CreateInDocIterator() const { return NULL; }
    void Free() {}
};

InDocPositionIteratorStateTest::InDocPositionIteratorStateTest()
{
}

InDocPositionIteratorStateTest::~InDocPositionIteratorStateTest()
{
}

void InDocPositionIteratorStateTest::CaseSetUp()
{
}

void InDocPositionIteratorStateTest::CaseTearDown()
{
}

void InDocPositionIteratorStateTest::TestCopy()
{
    InDocPositionIteratorStateMock state(SHORT_LIST_COMPRESS_MODE, of_doc_payload);
    PositionListSegmentDecoder decoder(of_doc_payload, (autil::mem_pool::Pool*)NULL);
    state.SetTermFreq(12);
    state.SetPositionListSegmentDecoder(&decoder);
    state.SetOffsetInRecord(6);
    state.SetTotalPosCount(11);
    state.SetRecordOffset(10);
    
    InDocPositionIteratorStateMock copiedState(state);
    ASSERT_EQ(state.GetTermFreq(), copiedState.GetTermFreq());
    ASSERT_EQ(state.GetPositionListSegmentDecoder(), 
              copiedState.GetPositionListSegmentDecoder());
    ASSERT_EQ(state.GetCompressMode(), copiedState.GetCompressMode());
    ASSERT_EQ(state.GetDocId(), copiedState.GetDocId());
    ASSERT_EQ(state.GetTotalPosCount(), copiedState.GetTotalPosCount());
    ASSERT_EQ(state.GetRecordOffset(), copiedState.GetRecordOffset());
}

IE_NAMESPACE_END(index);

