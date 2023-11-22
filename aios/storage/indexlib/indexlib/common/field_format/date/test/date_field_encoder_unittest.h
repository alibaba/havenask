#pragma once

#include "indexlib/common/field_format/date/date_field_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib::common {

class DateFieldEncoderTest : public INDEXLIB_TESTBASE
{
public:
    DateFieldEncoderTest();
    ~DateFieldEncoderTest();

    DECLARE_CLASS_NAME(DateFieldEncoderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestCreateTerms(config::IndexPartitionSchemaPtr schema, const std::string& timestamp,
                              const std::string& terms, const config::DateLevelFormat& format);
    void CreateTerms(const std::string& terms, std::vector<index::DateTerm>& dateTerms);
    void CreateTerms(const std::string& terms, std::vector<dictkey_t>& dateTerms);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateFieldEncoderTest, TestSimpleProcess);
} // namespace indexlib::common
