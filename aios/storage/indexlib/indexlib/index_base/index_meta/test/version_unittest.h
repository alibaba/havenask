#pragma once
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class VersionTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VersionTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

    void TestCaseForFromString();
    void TestCaseForFromStringWithTimestamp();
    void TestCaseForFromStringWithBadStr();
    void TestCaseForToString();
    void TestCaseForAssignOperator();
    void TestCaseForMinus();
    void TestCaseForAddSegmentException();
    void TestValidateSegmentIds();
    void TestStore();
    void TestCaseForFromStringWithLevelInfo();
    void TestCaseForAddSegment();
    void TestCaseForRemoveSegment();
    void TestFromStringWithoutLevelInfo();
    void TestSegmentStatistics();
    void TestGetSegmentStatsBySegmentId();

private:
    void CheckEqual(const Version& v1, const Version& v2);
    void DoTestMinus(const std::string& left, const std::string& right, const std::string& expect);
    Version::SegmentIdVec CreateSegmentVector(const std::string& data);
};

INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForFromString);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForFromStringWithBadStr);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForToString);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForFromStringWithTimestamp);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForAssignOperator);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForMinus);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForAddSegmentException);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestValidateSegmentIds);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestStore);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForFromStringWithLevelInfo);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForAddSegment);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestCaseForRemoveSegment);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestFromStringWithoutLevelInfo);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestSegmentStatistics);
INDEXLIB_UNIT_TEST_CASE(VersionTest, TestGetSegmentStatsBySegmentId);
}} // namespace indexlib::index_base
