#ifndef __INDEXLIB_ATTRIBUTEUPDATEINFOTEST_H
#define __INDEXLIB_ATTRIBUTEUPDATEINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"

IE_NAMESPACE_BEGIN(index);

class AttributeUpdateInfoTest : public INDEXLIB_TESTBASE
{
public:
    AttributeUpdateInfoTest();
    ~AttributeUpdateInfoTest();

    DECLARE_CLASS_NAME(AttributeUpdateInfoTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeUpdateInfoTest, TestJsonize);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEUPDATEINFOTEST_H
