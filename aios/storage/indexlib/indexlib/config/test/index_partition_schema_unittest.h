#pragma once
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class IndexPartitionSchemaTest : public INDEXLIB_TESTBASE
{
public:
    IndexPartitionSchemaTest() {}
    ~IndexPartitionSchemaTest() {}

    DECLARE_CLASS_NAME(IndexPartitionSchemaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForJsonize();
    void TestCaseForFieldConfig();
    void TestCaseForGetIndexIdList();
    void TestCaseForDictSchema();
    void TestCaseForCheck();
    void TestCaseFor32FieldCountInPack();
    void TestCaseFor33FieldCountInPack();
    void TestCaseFor9FieldCountInExpack();
    void TestCaseFor8FieldCountInExpack();
    void TestCaseForJsonizeWithTruncate();
    void TestCaseForTruncate();
    void TestCaseForTruncateCheck();
    void TestCaseForJsonizeWithSubSchema();
    void TestAssertCompatible();
    void TestCaseForIsUsefulField();
    void TestCaseForVirtualField();
    void TestCaseForVirtualAttribute();
    void TestCaseForPackAttribute();
    void TestCaseForPackAttributeWithError();
    void TestCaseForKVTTL();
    void TestCaseForNormalTTL();
    void TestCaseForEnableTTL();
    void TestCaseForAddVirtualAttributeConfig();
    void TestCaseForCloneVirtualAttributes();
    void TestCaseForKVIndex();
    void TestCaseForKVIndexException();
    void TestCaseForKKVIndex();
    void TestCaseForKKVIndexException();
    void TestCaseForUserDefinedParam();
    void TestCaseForMultiRegionKVSchema();
    void TestCaseForOrderPreservingField();
    void TestCaseForMultiRegionKKVSchema();
    void TestCaseForMultiRegionKVSchemaWithRegionFieldSchema();
    void TestCaseForNoIndexException();
    void TestCaseForCustomizedTable();
    void TestErrorCaseForCustomizedTable();
    void TestCaseForModifyOperation();
    void TestCaseForMarkOngoingOperation();
    void TestCaseForInvalidModifyOperation();
    void TestCaseForAddNonVirtualException();
    void TestCaseForCompitableWithModifyOperation();
    void TestCaseForModifyOperationWithTruncateIndex();
    void TestCaseForAddSpatialIndex();
    void TestDeleteMultiTimes();
    void TestDeleteTruncateIndex();
    void TestDeleteAttribute();
    void TestCreateIterator();
    void TestEffectiveIndexInfo();
    void TestSupportAutoUpdate();
    void TestSupportNullField();
    void TestCaseForTimestamp();
    void TestDisableOperation();
    void TestOperationNoAdd();
    void TestSourceSchema();
    void TestSchemaWithHashId();
    void TestSchemaForShortListVbyteCompress();
    void TestSchemaForIndexFormatVersionIdOverRange();
    void TestDefaultIndexFormatVersion();
    void TestCaseForNewAttributeSchema();
    void TestCaseForInvalidNewAttributeSchema();
    void TestCaseForNewAttributeLegacyWrite();
    void TestDifferentDefaultVersionIdByEnv();
    void TestCaseForIndexWithFileCompress();
    void TestCaseForTemperatureLayer();
    void TestCaseForTemperatureLayerWithFileCompress();
    void TestCaseForUpdateFileCompressSchema();
    void TestCaseForOrc();
    void TestCaseForNoIndexKv();

private:
    IndexPartitionSchemaPtr ReadSchema(const std::string& fileName);
    void CheckVirtualAttribute(const AttributeSchemaPtr& virtualAttrSchema, const std::string& fieldName,
                               fieldid_t fieldId);

    void CheckFieldSchema(const FieldSchemaPtr& schema, const std::string& infoStr);
    void CheckIndexSchema(const IndexSchemaPtr& schema, const std::string& infoStr);
    void CheckAttributeSchema(const AttributeSchemaPtr& schema, const std::string& infoStr);

    void CheckIterator(const IndexPartitionSchemaPtr& schema,
                       const std::string& target, // index | attribute | field
                       IndexStatus type, const std::vector<std::string>& expectNames);

    void CheckEffectiveIndexInfo(const IndexPartitionSchemaPtr& schema, schema_opid_t opId,
                                 const std::vector<schema_opid_t>& ongoingId, const std::string& expectIndexs,
                                 const std::string& expectAttrs, bool onlyModifyItem = false);
};

INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForJsonizeWithSubSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForJsonize);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForFieldConfig);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForCheck);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseFor32FieldCountInPack);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseFor33FieldCountInPack);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseFor9FieldCountInExpack);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseFor8FieldCountInExpack);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForDictSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForJsonizeWithTruncate);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForTruncate);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForTruncateCheck);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForGetIndexIdList);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestAssertCompatible);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForIsUsefulField);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForVirtualField);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForVirtualAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForPackAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForPackAttributeWithError);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForNormalTTL);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForKVTTL);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForEnableTTL);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForAddVirtualAttributeConfig);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForCloneVirtualAttributes);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForKVIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForKVIndexException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForKKVIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForKKVIndexException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForUserDefinedParam);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForMultiRegionKVSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForMultiRegionKKVSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForOrderPreservingField);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForMultiRegionKVSchemaWithRegionFieldSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForNoIndexException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForCustomizedTable);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestErrorCaseForCustomizedTable);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForModifyOperation);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForMarkOngoingOperation);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForInvalidModifyOperation);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForAddNonVirtualException);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForCompitableWithModifyOperation);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDeleteMultiTimes);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForAddSpatialIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDeleteTruncateIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDeleteAttribute);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCreateIterator);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestEffectiveIndexInfo);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSupportAutoUpdate);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSupportNullField);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForTimestamp);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDisableOperation);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestOperationNoAdd);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSourceSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSchemaWithHashId);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForModifyOperationWithTruncateIndex);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSchemaForShortListVbyteCompress);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestSchemaForIndexFormatVersionIdOverRange);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDefaultIndexFormatVersion);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForNewAttributeSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForInvalidNewAttributeSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForNewAttributeLegacyWrite);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestDifferentDefaultVersionIdByEnv);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForIndexWithFileCompress);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForTemperatureLayer);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForTemperatureLayerWithFileCompress);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForUpdateFileCompressSchema);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForOrc);
INDEXLIB_UNIT_TEST_CASE(IndexPartitionSchemaTest, TestCaseForNoIndexKv);

}} // namespace indexlib::config
