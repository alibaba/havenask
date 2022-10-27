#ifndef __INDEXLIB_MODIFYOPERATIONINTETEST_H
#define __INDEXLIB_MODIFYOPERATIONINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include <cmath>

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);
                   
IE_NAMESPACE_BEGIN(partition);

//first bool enable sharding index
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
    void TestMultiRoundDeleteAndUpdate();
    void TestOngoingOpWithCustomizeIndex();
    void TestMultiRoundDeleteAndUpdateForFloatCompress();    
    
private:
    void CheckLatestVersion(const std::string& rootPath,
                            schemavid_t expectSchemaId,
                            const std::vector<schema_opid_t>& ongoingOpIds);

    void CheckQuery(const std::string& rootPath,
                    const std::string& query,
                    const std::string& result,
                    const std::string& pluginPath = "");

    void PreparePatchIndex(const config::IndexPartitionSchemaPtr& newSchema,
                           const std::string& attribute, uint32_t delta);

    void FillOneAttributeForSegment(const PartitionPatcherPtr& patcher,
                                    const PartitionIteratorPtr& iter,
                                    const std::string& attrName, segmentid_t segId);
private:
    config::IndexPartitionSchemaPtr mNewSchema;
    util::QuotaControlPtr mQuotaControl;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MODIFYOPERATIONINTETEST_H
