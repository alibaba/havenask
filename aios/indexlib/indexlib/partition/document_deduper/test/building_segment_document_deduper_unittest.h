#ifndef __INDEXLIB_BUILDINGSEGMENTDOCUMENTDEDUPERTEST_H
#define __INDEXLIB_BUILDINGSEGMENTDOCUMENTDEDUPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/document_deduper/building_segment_document_deduper.h"

IE_NAMESPACE_BEGIN(partition);

class BuildingSegmentDocumentDeduperTest : public INDEXLIB_TESTBASE
{
public:
    BuildingSegmentDocumentDeduperTest();
    ~BuildingSegmentDocumentDeduperTest();

    DECLARE_CLASS_NAME(BuildingSegmentDocumentDeduperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRecordDocids();
    void TestDedup64Pks();
    void TestDedup128Pks();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildingSegmentDocumentDeduperTest, TestRecordDocids);
INDEXLIB_UNIT_TEST_CASE(BuildingSegmentDocumentDeduperTest, TestDedup64Pks);
INDEXLIB_UNIT_TEST_CASE(BuildingSegmentDocumentDeduperTest, TestDedup128Pks);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_BUILDINGSEGMENTDOCUMENTDEDUPERTEST_H
