#pragma once

#include <cmath>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

namespace indexlib { namespace partition {

// first bool enable sharding index
class ModifyOperationInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    ModifyOperationInteTest();
    ~ModifyOperationInteTest();

    DECLARE_CLASS_NAME(ModifyOperationInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBuildWithModifyOperation();
    void TestIncParallelBuildWithModifyOperation();
    void TestMultiRoundDeleteAndUpdate();
    void TestOngoingOpWithCustomizeIndex();
    void TestMultiRoundDeleteAndUpdateForFloatCompress();
    void TestModifyOperationSupportNullField();
    void TestModifyOperationForTimeAttribute();

private:
    void CheckLatestVersion(const indexlib::file_system::DirectoryPtr& rootDir, schemaid_t expectSchemaId,
                            const std::vector<schema_opid_t>& ongoingOpIds);

    void CheckQuery(const indexlib::file_system::DirectoryPtr& rootDir, const std::string& query,
                    const std::string& result, const std::string& pluginPath = "");

    void PreparePatchIndex(const config::IndexPartitionSchemaPtr& newSchema, const std::string& attribute,
                           uint32_t delta);

    void FillOneAttributeForSegment(const PartitionPatcherPtr& patcher, const PartitionIteratorPtr& iter,
                                    const std::string& attrName, segmentid_t segId);

    void InnerTestModifyOperationSupportNullField(const std::string& fieldInfo, const std::string& indexType);

    void InnerTestModifyOperationForTimeAttribute(const std::string& fieldInfo, const std::string& indexType);

private:
    config::IndexPartitionSchemaPtr mNewSchema;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
