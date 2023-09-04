#ifndef __INDEXLIB_FIELDTYPETRAITSTEST_H
#define __INDEXLIB_FIELDTYPETRAITSTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {
class FieldTypeTraitsTest : public INDEXLIB_TESTBASE
{
public:
    FieldTypeTraitsTest();
    ~FieldTypeTraitsTest();

    DECLARE_CLASS_NAME(FieldTypeTraitsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFieldToAttrType();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FieldTypeTraitsTest, TestFieldToAttrType);
}} // namespace indexlib::config

#endif //__INDEXLIB_FIELDTYPETRAITSTEST_H
