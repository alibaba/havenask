#ifndef __INDEXLIB_VIRTUALATTRIBUTEDATAAPPENDERTEST_H
#define __INDEXLIB_VIRTUALATTRIBUTEDATAAPPENDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VirtualAttributeDataAppenderTest : public INDEXLIB_TESTBASE
{
public:
    VirtualAttributeDataAppenderTest();
    ~VirtualAttributeDataAppenderTest();

    DECLARE_CLASS_NAME(VirtualAttributeDataAppenderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCheckDataExist();
    void TestAppendVirutalAttributeWithoutAttribute();

private:
    void CheckAttribute(const MultiFieldAttributeReader& attrReaders, const std::string& attrName, uint32_t docCount,
                        const std::string& expectValue);

    config::AttributeConfigPtr CreateAttributeConfig(const std::string& name, FieldType type,
                                                     const std::string& defaultValue);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VirtualAttributeDataAppenderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(VirtualAttributeDataAppenderTest, TestCheckDataExist)
INDEXLIB_UNIT_TEST_CASE(VirtualAttributeDataAppenderTest, TestAppendVirutalAttributeWithoutAttribute);
}} // namespace indexlib::index

#endif //__INDEXLIB_VIRTUALATTRIBUTEDATAAPPENDERTEST_H
