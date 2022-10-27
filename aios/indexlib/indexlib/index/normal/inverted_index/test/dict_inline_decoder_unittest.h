#ifndef __INDEXLIB_DICTINLINEDECODERTEST_H
#define __INDEXLIB_DICTINLINEDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_decoder.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineDecoderTest : public INDEXLIB_TESTBASE {
public:
    DictInlineDecoderTest();
    ~DictInlineDecoderTest();

    DECLARE_CLASS_NAME(DictInlineDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictInlineDecoderTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTINLINEDECODERTEST_H
