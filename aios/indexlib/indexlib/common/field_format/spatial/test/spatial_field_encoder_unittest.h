#ifndef __INDEXLIB_SPATIALFIELDENCODERTEST_H
#define __INDEXLIB_SPATIALFIELDENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/spatial_field_encoder.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(common);

class SpatialFieldEncoderTest : public INDEXLIB_TESTBASE
{
public:
    SpatialFieldEncoderTest();
    ~SpatialFieldEncoderTest();

    DECLARE_CLASS_NAME(SpatialFieldEncoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestIsSpatialIndexField();
    void TestEncodePoint();
    void TestEncodeShape();
    void TestEncodeNonSpatialIndexField();

private:
    void ExtractExpectedTerms(const std::string& notLeafGeoHashStr,
                              const std::string& leafGeoHashStr,
                              std::set<dictkey_t>& terms);
    bool CheckTerms(const std::vector<dictkey_t>& terms,
                    const std::set<dictkey_t>& expectedTerms);

private:
    config::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialFieldEncoderTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(SpatialFieldEncoderTest, TestIsSpatialIndexField);
INDEXLIB_UNIT_TEST_CASE(SpatialFieldEncoderTest, TestEncodePoint);
INDEXLIB_UNIT_TEST_CASE(SpatialFieldEncoderTest, TestEncodeShape);
INDEXLIB_UNIT_TEST_CASE(SpatialFieldEncoderTest, TestEncodeNonSpatialIndexField);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPATIALFIELDENCODERTEST_H
