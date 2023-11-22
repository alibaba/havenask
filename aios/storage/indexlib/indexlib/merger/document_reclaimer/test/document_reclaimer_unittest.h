#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/merger/document_reclaimer/document_reclaimer.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::index_base;
namespace indexlib { namespace merger {

class DocumentReclaimerTest : public INDEXLIB_TESTBASE
{
public:
    DocumentReclaimerTest();
    ~DocumentReclaimerTest();

    DECLARE_CLASS_NAME(DocumentReclaimerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSinglePartitionMerge();
    void TestSinglePartitionMergeWithReclaimOperator();
    void TestSinglePartitionMergeWithReclaimOperatorWithSpecialParam();
    void TestMultiPartitionMerge();
    void TestMultiPartitionMergeWithReclaimOperator();
    void TestSinglePartitionMergeWithSubDoc();
    void TestSinglePartitionMergeWithRecover();
    void TestMultiPartitionMergeWithRecover();
    void TestDumplicateIndexName();
    void TestDumplicateTerms();
    void TestReclaimByPKIndex();
    void TestConfigFileExceptions();
    void TestReopenReclaimSegment();
    void testReclaimOldTTLDoc();
    void testSearchTermFailInAndQuery();

public:
    static void DumpIndexReclaimerParams(const std::string& param);

private:
    void SetDocReclaimParam(const std::string& param);
    void PrepareReclaimConfig(const std::string& param);
    bool FindFromDeployIndex(const std::string& filePath, file_system::DeployIndexMeta& deployIndex) const;

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestSinglePartitionMerge);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestSinglePartitionMergeWithReclaimOperator);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestSinglePartitionMergeWithReclaimOperatorWithSpecialParam);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestMultiPartitionMerge);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestMultiPartitionMergeWithReclaimOperator);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestSinglePartitionMergeWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestSinglePartitionMergeWithRecover);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestMultiPartitionMergeWithRecover);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestDumplicateIndexName);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestDumplicateTerms);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestReclaimByPKIndex);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestConfigFileExceptions);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, TestReopenReclaimSegment);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, testReclaimOldTTLDoc);
INDEXLIB_UNIT_TEST_CASE(DocumentReclaimerTest, testSearchTermFailInAndQuery);
}} // namespace indexlib::merger
