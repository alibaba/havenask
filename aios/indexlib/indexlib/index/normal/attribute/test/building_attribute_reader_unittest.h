#ifndef __INDEXLIB_BUILDINGATTRIBUTEREADERTEST_H
#define __INDEXLIB_BUILDINGATTRIBUTEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/building_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/test/fake_partition_data_creator.h"

IE_NAMESPACE_BEGIN(index);

class BuildingAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    BuildingAttributeReaderTest();
    ~BuildingAttributeReaderTest();

    DECLARE_CLASS_NAME(BuildingAttributeReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    
    void TestSingleAttributeReader();
    void TestVarNumAttributeReader();
    void TestSingleStringAttributeReader();
    void TestMultiStringAttributeReader();

private:
    void CheckSingleReader(SingleValueAttributeReader<uint64_t>& reader,
                           const std::string& resultStr);
    
    void CheckVarNumReader(VarNumAttributeReader<uint32_t>& reader,
                           const std::string& resultStr);

    void CheckStringReader(StringAttributeReader& reader,
                           const std::string& resultStr);

    void CheckMultiStringReader(MultiStringAttributeReader& reader,
                                const std::string& resultStr);
private:
    config::IndexPartitionSchemaPtr mSchema;
    test::FakePartitionDataCreatorPtr mPdc;
    index_base::PartitionDataPtr mPartitionData;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildingAttributeReaderTest, TestSingleAttributeReader);
INDEXLIB_UNIT_TEST_CASE(BuildingAttributeReaderTest, TestVarNumAttributeReader);
INDEXLIB_UNIT_TEST_CASE(BuildingAttributeReaderTest, TestSingleStringAttributeReader);
INDEXLIB_UNIT_TEST_CASE(BuildingAttributeReaderTest, TestMultiStringAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDINGATTRIBUTEREADERTEST_H
