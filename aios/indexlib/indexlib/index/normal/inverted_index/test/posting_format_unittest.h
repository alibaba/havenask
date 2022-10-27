#ifndef __INDEXLIB_POSTINGFORMATTEST_H
#define __INDEXLIB_POSTINGFORMATTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"

IE_NAMESPACE_BEGIN(index);

class PostingFormatTest : public INDEXLIB_TESTBASE {
public:
    PostingFormatTest();
    ~PostingFormatTest();

    DECLARE_CLASS_NAME(PostingFormatTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDocListFormat();
    void TestPositionListFormat();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingFormatTest, TestPositionListFormat);
INDEXLIB_UNIT_TEST_CASE(PostingFormatTest, TestDocListFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGFORMATTEST_H
