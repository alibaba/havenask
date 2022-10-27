#ifndef __INDEXLIB_BUILTSEGMENTSDOCUMENTDEDUPERTEST_H
#define __INDEXLIB_BUILTSEGMENTSDOCUMENTDEDUPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"

IE_NAMESPACE_BEGIN(partition);

class BuiltSegmentsDocumentDeduperTest : public INDEXLIB_TESTBASE
{
public:
    BuiltSegmentsDocumentDeduperTest();
    ~BuiltSegmentsDocumentDeduperTest();

    DECLARE_CLASS_NAME(BuiltSegmentsDocumentDeduperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDedup64Pks();
    void TestDedup128Pks();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuiltSegmentsDocumentDeduperTest, TestDedup64Pks);
INDEXLIB_UNIT_TEST_CASE(BuiltSegmentsDocumentDeduperTest, TestDedup128Pks);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_BUILTSEGMENTSDOCUMENTDEDUPERTEST_H
