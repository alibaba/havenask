#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_index_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index { namespace legacy {

class SpatialIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    SpatialIndexReaderTest();
    ~SpatialIndexReaderTest();

    DECLARE_CLASS_NAME(SpatialIndexReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestParseShape();
    void TestParseShapeFail();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialIndexReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexReaderTest, TestParseShape);
INDEXLIB_UNIT_TEST_CASE(SpatialIndexReaderTest, TestParseShapeFail);
}}} // namespace indexlib::index::legacy
