#ifndef __INDEXLIB_NORMALINDOCSTATETEST_H
#define __INDEXLIB_NORMALINDOCSTATETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"

IE_NAMESPACE_BEGIN(index);

class NormalInDocStateTest : public INDEXLIB_TESTBASE {
public:
    NormalInDocStateTest();
    ~NormalInDocStateTest();
public:
    void SetUp();
    void TearDown();
    void TestSetSectionReader();
    void TestAlignSize();
    void TestCopy();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NormalInDocStateTest, TestSetSectionReader);
INDEXLIB_UNIT_TEST_CASE(NormalInDocStateTest, TestAlignSize);
INDEXLIB_UNIT_TEST_CASE(NormalInDocStateTest, TestCopy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMALINDOCSTATETEST_H
