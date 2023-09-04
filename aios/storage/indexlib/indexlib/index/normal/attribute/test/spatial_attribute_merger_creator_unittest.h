#ifndef __INDEXLIB_SPATIALATTRIBUTEMERGERCREATORTEST_H
#define __INDEXLIB_SPATIALATTRIBUTEMERGERCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SpatialAttributeMergerCreatorTest : public INDEXLIB_TESTBASE
{
public:
    SpatialAttributeMergerCreatorTest();
    ~SpatialAttributeMergerCreatorTest();

    DECLARE_CLASS_NAME(SpatialAttributeMergerCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialAttributeMergerCreatorTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_SPATIALATTRIBUTEMERGERCREATORTEST_H
