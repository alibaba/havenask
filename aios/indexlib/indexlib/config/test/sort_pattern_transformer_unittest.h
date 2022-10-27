#ifndef __INDEXLIB_SORTPATTERNTRANSFORMERTEST_H
#define __INDEXLIB_SORTPATTERNTRANSFORMERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/sort_pattern_transformer.h"

IE_NAMESPACE_BEGIN(config);

class SortPatternTransformerTest : public INDEXLIB_TESTBASE
{
public:
    SortPatternTransformerTest();
    ~SortPatternTransformerTest();

    DECLARE_CLASS_NAME(SortPatternTransformerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortPatternTransformerTest, TestSimpleProcess);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SORTPATTERNTRANSFORMERTEST_H
