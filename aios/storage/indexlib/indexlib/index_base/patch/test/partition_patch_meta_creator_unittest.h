#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/partition_patch_meta_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class PartitionPatchMetaCreatorTest : public INDEXLIB_TESTBASE
{
public:
    PartitionPatchMetaCreatorTest();
    ~PartitionPatchMetaCreatorTest();

    DECLARE_CLASS_NAME(PartitionPatchMetaCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSchemaAndVersionNotSameSchemaVersion();
    void TestMultiSchemaIdPatchMeta();
    void TestMultiSchemaIdPatchMetaWithLastVersion();
    void TestWithInvalidIndexAndAttribute();
    void TestSchemaWithModifyOperations();

private:
    index_base::Version CreateVersion(const std::string& versionStr, versionid_t versionId);

    // dataStr: segId0:[indexs]:[attrs];...;segIdN:[indexs]:[attrs]
    // [indexs] : index1,index2,index3,...
    // [attrs] : attr1,attr2,attr3,...
    void PreparePatchIndex(const file_system::DirectoryPtr& rootDir, schemaid_t schemaId, const std::string& dataStr);

    void CheckPatchMeta(const PartitionPatchMetaPtr& patchMeta, schemaid_t schemaId, const std::string& dataStr);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestSchemaAndVersionNotSameSchemaVersion);
INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestMultiSchemaIdPatchMeta);
INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestMultiSchemaIdPatchMetaWithLastVersion);
INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestWithInvalidIndexAndAttribute);
INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaCreatorTest, TestSchemaWithModifyOperations);
}} // namespace indexlib::index_base
