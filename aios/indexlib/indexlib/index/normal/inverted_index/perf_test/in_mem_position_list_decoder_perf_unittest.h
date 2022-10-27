#ifndef __INDEXLIB_INMEMPOSITIONLISTDECODERPERFTEST_H
#define __INDEXLIB_INMEMPOSITIONLISTDECODERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(index);

class InMemPositionListDecoderPerfTest : public test::MultiThreadTestBase
{
public:
    InMemPositionListDecoderPerfTest();
    ~InMemPositionListDecoderPerfTest();

    DECLARE_CLASS_NAME(InMemPositionListDecoderPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetInMemPositionListDecoder();

private:
    void DoWrite() override;
    void DoRead(int* errCode) override;

    pos_t GetPositionDeltaByIdx(size_t idx)
    {
        if(idx)
        {
            return (pos_t)(idx % 1331) + 1;
        }
        return 0;
    }

private:
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;
    PositionListEncoderPtr mEncoder;
    std::string mDir;
    ttf_t volatile mGlobalTTF;
    bool volatile mIsDumped;
    optionflag_t mFlag;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemPositionListDecoderPerfTest, TestGetInMemPositionListDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMPOSITIONLISTDECODERPERFTEST_H
