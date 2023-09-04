#ifndef __INDEXLIB_SPATIALINDEXINTETEST_H
#define __INDEXLIB_SPATIALINDEXINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/raw_document_field_extractor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

namespace indexlib { namespace index {

class SpatialIndexInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<bool, size_t>>
{
public:
    SpatialIndexInteTest();
    ~SpatialIndexInteTest();

    DECLARE_CLASS_NAME(SpatialIndexInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestFindPointNear180DateLine();
    void TestLocationOnCellRightBoundary();
    void TestFindPointsInCircle();
    void TestFindNearPole();
    void TestSearchTopLevel();
    void TestAttributeInputIllegal();
    void TestSeekAndFilterIterator();
    void TestLookupAsync();
    void TestPolygonSearch();
    void TestPolygonLeafSearch();
    void TestLineSearch();
    void TestLazyloadSpatialAttribute();
    void TestSchemaMaker();
    void TestMergeMulitSegment();

private:
    config::IndexPartitionSchemaPtr RewriteSpatialConfig(const config::IndexPartitionSchemaPtr& schema,
                                                         double maxSearchDist, double minDistError,
                                                         const std::string& indexName);

    uint32_t GetMatchDocCount(PostingIterator* iter);
    void InnerCheckIterator(partition::RawDocumentFieldExtractor& extractor, docid_t docId,
                            const std::vector<std::string>& fieldNames, const std::string& fieldValues);

    void CheckSpatialIndexCompressInfo(const file_system::IFileSystemPtr& fileSystem);

    void CheckFileCompressInfo(const index_base::PartitionDataPtr& partitionData, const std::string& filePath,
                               const std::string& compressType);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    bool mEnableFileCompress;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(SpatialIndexInteTestMode, SpatialIndexInteTest,
                                     testing::Combine(testing::Bool(), testing::Values(0, 1, 2)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestFindPointNear180DateLine);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestLocationOnCellRightBoundary);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestFindPointsInCircle);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestFindNearPole);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestSearchTopLevel);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestAttributeInputIllegal);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestSeekAndFilterIterator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestLookupAsync);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestPolygonSearch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestPolygonLeafSearch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestLineSearch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestLazyloadSpatialAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestSchemaMaker);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SpatialIndexInteTestMode, TestMergeMulitSegment);
}} // namespace indexlib::index

#endif //__INDEXLIB_SPATIALINDEXINTETEST_H
