#ifndef __INDEXLIB_DICTINLINEFORMATTERTEST_H
#define __INDEXLIB_DICTINLINEFORMATTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineFormatterTest : public INDEXLIB_TESTBASE {
public:
    DictInlineFormatterTest();
    ~DictInlineFormatterTest();

    DECLARE_CLASS_NAME(DictInlineFormatterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestToVector();
    void TestFromVector();
    void TestCalculateDictInlineItemCount();

private:
    void CheckFormatterElements(
            uint32_t expectData[], DictInlineFormatter& mapper);
    void CheckVectorEqual(
            const std::vector<uint32_t>& leftVector,
            const std::vector<uint32_t>& rightVector);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DictInlineFormatterTest, TestToVector);
INDEXLIB_UNIT_TEST_CASE(DictInlineFormatterTest, TestFromVector);
INDEXLIB_UNIT_TEST_CASE(DictInlineFormatterTest, TestCalculateDictInlineItemCount);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTINLINEFORMATTERTEST_H
