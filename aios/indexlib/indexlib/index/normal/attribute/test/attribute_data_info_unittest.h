#ifndef __INDEXLIB_ATTRIBUTEDATAINFOTEST_H
#define __INDEXLIB_ATTRIBUTEDATAINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"

IE_NAMESPACE_BEGIN(index);

class AttributeDataInfoTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDataInfoTest();
    ~AttributeDataInfoTest();

    DECLARE_CLASS_NAME(AttributeDataInfoTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDataInfoTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEDATAINFOTEST_H
