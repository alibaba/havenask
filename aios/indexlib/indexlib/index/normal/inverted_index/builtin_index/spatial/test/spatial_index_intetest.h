#ifndef __INDEXLIB_SPATIALINDEXINTETEST_H
#define __INDEXLIB_SPATIALINDEXINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/raw_document_field_extractor.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

IE_NAMESPACE_BEGIN(index);

class SpatialIndexInteTest : public INDEXLIB_TESTBASE
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
    void TestPolygonSearch();
    void TestPolygonLeafSearch();
    void TestLineSearch();
    void TestLazyloadSpatialAttribute();
    void TestSchemaMaker();
    void TestMergeMulitSegment();

private:
    void RewriteSpatialConfig(const config::IndexPartitionSchemaPtr& schema, 
                              double maxSearchDist, double minDistError,
                              const std::string& indexName);

    uint32_t GetMatchDocCount(PostingIterator* iter);
    void InnerCheckIterator(
        partition::RawDocumentFieldExtractor& extractor, docid_t docId,
        const std::vector<std::string>& fieldNames,
        const std::string& fieldValues);
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestFindPointNear180DateLine);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestLocationOnCellRightBoundary);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestFindPointsInCircle);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestFindNearPole);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestSearchTopLevel);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestAttributeInputIllegal);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestSeekAndFilterIterator);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestPolygonSearch);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestPolygonLeafSearch);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestLineSearch);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestLazyloadSpatialAttribute);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestSchemaMaker);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexInteTest, TestMergeMulitSegment);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIALINDEXINTETEST_H
