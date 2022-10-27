#ifndef __INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H
#define __INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/section_attribute_config.h"

IE_NAMESPACE_BEGIN(config);

class SectionAttributeConfigTest : public INDEXLIB_TESTBASE {
public:
    SectionAttributeConfigTest();
    ~SectionAttributeConfigTest();
public:
    void SetUp();
    void TearDown();
    void TestJsonize();
    void TestAssertEqual();
    void TestEqual();
    void TestCreateAttributeConfig();
    void TestSectionAttributeNameToIndexName();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeConfigTest, TestAssertEqual);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeConfigTest, TestEqual);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeConfigTest, TestCreateAttributeConfig);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeConfigTest, TestSectionAttributeNameToIndexName);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H
