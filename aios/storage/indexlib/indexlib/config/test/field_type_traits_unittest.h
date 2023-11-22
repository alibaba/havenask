#pragma once

#include "autil/Log.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/util/testutil/unittest.h"

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FieldTypeTraitsTest, TestFieldToAttrType);
}} // namespace indexlib::config
