#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class PackAttributeAppenderTest : public INDEXLIB_TESTBASE
{
public:
    PackAttributeAppenderTest();
    ~PackAttributeAppenderTest();

    DECLARE_CLASS_NAME(PackAttributeAppenderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestForKKVTable();

private:
    config::IndexPartitionSchemaPtr mSchema;
    document::NormalDocumentPtr mDoc;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackAttributeAppenderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PackAttributeAppenderTest, TestForKKVTable);
}} // namespace indexlib::document
