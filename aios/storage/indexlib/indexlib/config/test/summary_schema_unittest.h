#pragma once

#include "autil/Log.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

class SummarySchemaTest : public INDEXLIB_TESTBASE
{
public:
    SummarySchemaTest();
    ~SummarySchemaTest();

    DECLARE_CLASS_NAME(SummarySchemaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestSummaryGroup();
    void TestSummaryCompressor();
    void TestSummaryGroupWithParameter();
    void TestSummaryException();
    void TestJsonize();

private:
    std::string mRootDir;
    std::string mJsonStringHead;
    std::string mJsonStringTail;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestSummaryGroup);
INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestSummaryCompressor);
INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestSummaryException);
INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestSummaryGroupWithParameter);
INDEXLIB_UNIT_TEST_CASE(SummarySchemaTest, TestJsonize);
}} // namespace indexlib::config
