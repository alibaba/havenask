#ifndef __INDEXLIB_DICTINLINEPOSTINGDECODERTEST_H
#define __INDEXLIB_DICTINLINEPOSTINGDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_posting_decoder.h"

IE_NAMESPACE_BEGIN(index);

class DictInlinePostingDecoderTest : public INDEXLIB_TESTBASE {
public:
    DictInlinePostingDecoderTest();
    ~DictInlinePostingDecoderTest();

    DECLARE_CLASS_NAME(DictInlinePostingDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDecodeDocBuffer();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictInlinePostingDecoderTest, TestDecodeDocBuffer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTINLINEPOSTINGDECODERTEST_H
