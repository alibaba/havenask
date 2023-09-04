#ifndef __INDEXLIB_UPDATABLEVARNUMATTRIBUTEDATAFORMATTERTEST_H
#define __INDEXLIB_UPDATABLEVARNUMATTRIBUTEDATAFORMATTERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VarNumAttributeDataFormatterTest : public INDEXLIB_TESTBASE
{
public:
    VarNumAttributeDataFormatterTest();
    ~VarNumAttributeDataFormatterTest();

    DECLARE_CLASS_NAME(VarNumAttributeDataFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestGetNormalAttrDataLength();
    void TestGetNormalEncodeFloatAttrDataLength();
    void TestGetMultiStringAttrDataLength();

private:
    config::AttributeConfigPtr CreateAttrConfig(FieldType fieldType, bool IsMultiValue, bool IsUpdatable);

    void CheckFormatterInit(FieldType fieldType, bool IsMultiValue, bool IsUpdatable, uint32_t fieldSize);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataFormatterTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataFormatterTest, TestGetNormalAttrDataLength);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataFormatterTest, TestGetNormalEncodeFloatAttrDataLength);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDataFormatterTest, TestGetMultiStringAttrDataLength);
}} // namespace indexlib::index

#endif //__INDEXLIB_UPDATABLEVARNUMATTRIBUTEDATAFORMATTERTEST_H
