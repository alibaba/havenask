#pragma once

#include "indexlib/common_define.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class NewSchemaDropFieldTest : public INDEXLIB_TESTBASE
{
public:
    NewSchemaDropFieldTest();
    ~NewSchemaDropFieldTest();

    DECLARE_CLASS_NAME(NewSchemaDropFieldTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    bool CheckExist(const file_system::IndexFileList& deployMeta, const std::string& path);
    void InnerCheckIndex(const config::IndexPartitionSchemaPtr& schema, const std::string& docStr,
                         const std::string& query, const std::string& result);
    void PreparePatchIndex(const std::string& patchIndexPath, const config::IndexPartitionSchemaPtr& newSchema,
                           const std::vector<segmentid_t>& segmentList, const std::vector<std::string>& patchAttribute,
                           const std::vector<std::string>& patchIndex, versionid_t targetVersion);

private:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mIndexRoot;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NewSchemaDropFieldTest, TestSimpleProcess);
}} // namespace indexlib::partition
