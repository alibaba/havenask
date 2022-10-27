#ifndef __INDEXLIB_INDOCPOSITIONITERATORSTATETEST_H
#define __INDEXLIB_INDOCPOSITIONITERATORSTATETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator_state.h"

IE_NAMESPACE_BEGIN(index);

class InDocPositionIteratorStateTest : public INDEXLIB_TESTBASE {
public:
    InDocPositionIteratorStateTest();
    ~InDocPositionIteratorStateTest();

    DECLARE_CLASS_NAME(InDocPositionIteratorStateTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCopy();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InDocPositionIteratorStateTest, TestCopy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDOCPOSITIONITERATORSTATETEST_H
