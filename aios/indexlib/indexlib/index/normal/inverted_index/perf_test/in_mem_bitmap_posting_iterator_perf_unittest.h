#ifndef __INDEXLIB_INMEMBITMAPPOSTINGITERATORPERFTEST_H
#define __INDEXLIB_INMEMBITMAPPOSTINGITERATORPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"

IE_NAMESPACE_BEGIN(index);

class InMemBitmapPostingIteratorPerfTest : public test::MultiThreadTestBase
{
public:
    InMemBitmapPostingIteratorPerfTest();
    ~InMemBitmapPostingIteratorPerfTest();

    DECLARE_CLASS_NAME(InMemBitmapPostingIteratorPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetInMemBitmapPostingIterator();

private:
    void DoWrite() override;
    void DoRead(int* errCode) override;

    // return delta : docid[idx] - docid[idx - 1]
    int64_t GetDocIdDeltaByIdx(size_t idx)
    {
        return (int64_t)(idx % 13) + 1;
    }

private:
    autil::mem_pool::Pool mByteSlicePool;
    index::BitmapPostingWriterPtr mBitmapPostingWriter;
    std::string mDir;
    docid_t volatile mGlobalDocId;
    bool volatile mIsDumped;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemBitmapPostingIteratorPerfTest, TestGetInMemBitmapPostingIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMBITMAPPOSTINGITERATORPERFTEST_H
