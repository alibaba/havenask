#ifndef __INDEXLIB_POSTINGFORMATOPTIONTEST_H
#define __INDEXLIB_POSTINGFORMATOPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class PostingFormatOptionTest : public INDEXLIB_TESTBASE {
public:
    PostingFormatOptionTest();
    ~PostingFormatOptionTest();

    DECLARE_CLASS_NAME(PostingFormatOptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestEqual();
    void TestJsonize();
    void TestJsonizeCompatibleWithCompressMode();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingFormatOptionTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(PostingFormatOptionTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(PostingFormatOptionTest, TestJsonizeCompatibleWithCompressMode);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGFORMATOPTIONTEST_H
