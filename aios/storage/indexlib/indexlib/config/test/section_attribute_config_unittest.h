#ifndef __INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H
#define __INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class SectionAttributeConfigTest : public INDEXLIB_TESTBASE
{
public:
    SectionAttributeConfigTest();
    ~SectionAttributeConfigTest();

public:
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
}} // namespace indexlib::config

#endif //__INDEXLIB_SECTIONATTRIBUTECONFIGTEST_H
