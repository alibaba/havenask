#ifndef __INDEXLIB_INMEMDOCLISTDECODERPERFTEST_H
#define __INDEXLIB_INMEMDOCLISTDECODERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(index);

class InMemDocListDecoderPerfTest : public test::MultiThreadTestBase
{
public:
    InMemDocListDecoderPerfTest();
    ~InMemDocListDecoderPerfTest();

    DECLARE_CLASS_NAME(InMemDocListDecoderPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetInMemDocListDecoder();

private:
    void DoWrite() override;
    void DoRead(int* errCode) override;

    // return delta : docid[idx] - docid[idx - 1]
    docid_t GetDocIdDeltaByIdx(size_t idx)
    {
        if(idx)
        {
            return (docid_t)(idx % 1331) + 1;
        }
        return 0;
    }

private:
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;
    util::SimplePool mSimplePool;
    DocListEncoderPtr mEncoder;
    std::string mDir;
    size_t volatile mGlobalDocId;
    bool volatile mIsDumped;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemDocListDecoderPerfTest, TestGetInMemDocListDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMDOCLISTDECODERPERFTEST_H
