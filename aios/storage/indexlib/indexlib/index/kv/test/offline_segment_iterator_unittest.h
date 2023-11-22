#pragma once

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class OfflineSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OfflineSegmentIteratorTest();
    ~OfflineSegmentIteratorTest();

    DECLARE_CLASS_NAME(OfflineSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateHashTableIterator();
    void TestCreateMultiRegionHashTableIterator();

private:
    template <typename FieldType, bool hasTTL>
    void InnerTestCreateFixSegment(const std::string& field, const std::string& key, const std::string& value);

    template <typename FieldType, bool hasTTL>
    void InnerTestCreateVarSegment(const std::string& field, const std::string& key, const std::string& value);

    template <typename FieldType, bool hasTTL>
    void MultiRegionInnerTestCreate(std::vector<std::vector<std::string>>& kvPairs, const std::string& field);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OfflineSegmentIteratorTest, TestCreateHashTableIterator);
INDEXLIB_UNIT_TEST_CASE(OfflineSegmentIteratorTest, TestCreateMultiRegionHashTableIterator);
}} // namespace indexlib::index
