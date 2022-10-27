#ifndef __INDEXLIB_POSITIONLISTSEGMENTDECODERTEST_H
#define __INDEXLIB_POSITIONLISTSEGMENTDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"
#include "indexlib/file_system/in_mem_file_node.h"

IE_NAMESPACE_BEGIN(index);

class PositionListSegmentDecoderTest : public INDEXLIB_TESTBASE
{
public:
    PositionListSegmentDecoderTest();
    ~PositionListSegmentDecoderTest();

    DECLARE_CLASS_NAME(PositionListSegmentDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSkipAndLocateAndDecode();
    void TestSkipAndLocateAndDecodeWithTFBitmap();

private:
    void InnerTestSkipAndLocateAndDecode(optionflag_t optionFlag,
            const std::string& docStrs);

private:
    // docStrs : tf1,tf2
    util::ByteSliceList* PreparePositionList(
            optionflag_t optionFlag, const std::string& docStrs,
            int32_t& posListBegin, int32_t& posDataListBegin);

private:
    std::string mDir;
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;
    util::SimplePool mSimplePool;
    file_system::InMemFileNodePtr mFileReader;

    pos_t mPosBuffer[MAX_POS_PER_RECORD];
    pospayload_t mPosPayloadBuffer[MAX_POS_PER_RECORD];

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PositionListSegmentDecoderTest, TestSkipAndLocateAndDecodeWithTFBitmap);
INDEXLIB_UNIT_TEST_CASE(PositionListSegmentDecoderTest, TestSkipAndLocateAndDecode);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITIONLISTSEGMENTDECODERTEST_H
