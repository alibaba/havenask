#ifndef __INDEXLIB_VARNUMATTRIBUTEFORMATTERTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEFORMATTERTEST_H

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class VarNumAttributeFormatterTest : public INDEXLIB_TESTBASE
{
public:
    VarNumAttributeFormatterTest();
    ~VarNumAttributeFormatterTest();

    DECLARE_CLASS_NAME(VarNumAttributeFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestEncodeAndDecodeCount(uint32_t countValue);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeFormatterTest, TestSimpleProcess);
}} // namespace indexlib::common

#endif //__INDEXLIB_VARNUMATTRIBUTEFORMATTERTEST_H
