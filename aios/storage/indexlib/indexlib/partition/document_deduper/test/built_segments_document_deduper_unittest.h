#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
