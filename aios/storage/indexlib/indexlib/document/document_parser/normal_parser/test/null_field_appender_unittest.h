#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/normal_parser/null_field_appender.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class NullFieldAppenderTest : public INDEXLIB_TESTBASE
{
public:
    NullFieldAppenderTest();
    ~NullFieldAppenderTest();

    DECLARE_CLASS_NAME(NullFieldAppenderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NullFieldAppenderTest, TestSimpleProcess);
}} // namespace indexlib::document
