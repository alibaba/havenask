#ifndef __INDEXLIB_DICTINLINEENCODERTEST_H
#define __INDEXLIB_DICTINLINEENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineEncoderTest : public INDEXLIB_TESTBASE {
public:
    DictInlineEncoderTest();
    ~DictInlineEncoderTest();

    DECLARE_CLASS_NAME(DictInlineEncoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculateCompressedSize();
    void TestEncode();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictInlineEncoderTest, TestCalculateCompressedSize);
INDEXLIB_UNIT_TEST_CASE(DictInlineEncoderTest, TestEncode);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTINLINEENCODERTEST_H
