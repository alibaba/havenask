#ifndef __INDEXLIB_ANDINDEXRECLAIMERTEST_H
#define __INDEXLIB_ANDINDEXRECLAIMERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/document_reclaimer/and_index_reclaimer.h"

IE_NAMESPACE_BEGIN(merger);

class AndIndexReclaimerTest : public INDEXLIB_TESTBASE
{
public:
    AndIndexReclaimerTest();
    ~AndIndexReclaimerTest();

    DECLARE_CLASS_NAME(AndIndexReclaimerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AndIndexReclaimerTest, TestInit);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_ANDINDEXRECLAIMERTEST_H
