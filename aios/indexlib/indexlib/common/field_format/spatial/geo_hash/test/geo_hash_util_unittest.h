#ifndef __INDEXLIB_GEOHASHUTILTEST_H
#define __INDEXLIB_GEOHASHUTILTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

IE_NAMESPACE_BEGIN(common);

class GeoHashUtilTest : public INDEXLIB_TESTBASE
{
public:
    GeoHashUtilTest();
    ~GeoHashUtilTest();

    DECLARE_CLASS_NAME(GeoHashUtilTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEncode();
    void TestEncodeAbnormal();
    void TestDecodeAbnormal();
    void TestGeoHashStrToUInt64();
    void TestGetSubGeoHashIds();
    void TestDistanceToLevel();

private:
    void CheckEncode(double lon, double lat, int8_t level,
                     const std::string& str = std::string());

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestEncode);
INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestEncodeAbnormal);
INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestDecodeAbnormal);
INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestGeoHashStrToUInt64);
INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestGetSubGeoHashIds);
INDEXLIB_UNIT_TEST_CASE(GeoHashUtilTest, TestDistanceToLevel);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_GEOHASHUTILTEST_H
