#ifndef __INDEXLIB_SCHEMAMAKERTEST_H
#define __INDEXLIB_SCHEMAMAKERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace test {

class SchemaMakerTest : public INDEXLIB_TESTBASE
{
public:
    SchemaMakerTest();
    ~SchemaMakerTest();

    DECLARE_CLASS_NAME(SchemaMakerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SchemaMakerTest, TestSimpleProcess);
}} // namespace indexlib::test

#endif //__INDEXLIB_SCHEMAMAKERTEST_H
