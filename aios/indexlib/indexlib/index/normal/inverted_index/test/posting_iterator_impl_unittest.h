#ifndef __INDEXLIB_POSTINGITERATORIMPLTEST_H
#define __INDEXLIB_POSTINGITERATORIMPLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"

IE_NAMESPACE_BEGIN(index);

class PostingIteratorImplTest : public INDEXLIB_TESTBASE {
public:
    PostingIteratorImplTest();
    ~PostingIteratorImplTest();

    DECLARE_CLASS_NAME(PostingIteratorImplTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCopy();
private:
    IE_LOG_DECLARE();
};

//INDEXLIB_UNIT_TEST_CASE(PostingIteratorImplTest, TestCopy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGITERATORIMPLTEST_H
