#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"

#include <sstream>
#include <string>

#include "indexlib/index/common/field_format/spatial/geo_hash/geohash.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "unittest/unittest.h"

namespace indexlib::index {
class GeoHashUtilTest : public TESTBASE
{
public:
    GeoHashUtilTest() = default;
    ~GeoHashUtilTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void CheckEncode(double lon, double lat, int8_t level, const std::string& str);
};

void GeoHashUtilTest::CheckEncode(double lon, double lat, int8_t level, const std::string& geoHashStr)
{
    std::stringstream ss;
    ss << "lon=" << lon << ",lat=" << lat << ",level=" << (int32_t)level;

    // encode
    uint64_t actualHash = GeoHashUtil::Encode(lon, lat, level);
    ASSERT_EQ(level, GeoHashUtil::GetLevelOfHashId(actualHash)) << ss.str();
    if (!geoHashStr.empty()) {
        ASSERT_EQ(geoHashStr, GeoHashUtil::HashToGeoStr(actualHash)) << ss.str();
    }

    char* expectHashStr = GEOHASH_encode(lat, lon, level);
    ASSERT_TRUE(expectHashStr) << ss.str();
    uint64_t expectHash = GeoHashUtil::GeoStrToHash(expectHashStr);
    ASSERT_EQ(expectHash, actualHash) << ss.str();

    // decode
    double actualMinX;
    double actualMinY;
    double actualMaxX;
    double actualMaxY;
    GeoHashUtil::GetGeoHashArea(actualHash, actualMinX, actualMinY, actualMaxX, actualMaxY);
    GEOHASH_area* area = GEOHASH_decode(expectHashStr);
    ASSERT_TRUE(area) << ss.str();
    ASSERT_EQ(area->longitude.min, actualMinX) << ss.str();
    ASSERT_EQ(area->longitude.max, actualMaxX) << ss.str();
    ASSERT_EQ(area->latitude.min, actualMinY) << ss.str();
    ASSERT_EQ(area->latitude.max, actualMaxY) << ss.str();

    free(expectHashStr);
    GEOHASH_free_area(area);
}

TEST_F(GeoHashUtilTest, TestEncode)
{
    CheckEncode(0, 0, 1, "s");
    CheckEncode(0, 0, 11, "s0000000000");
    CheckEncode(-180, 0, 10, "8000000000");
    CheckEncode(180, 0, 9, "xbpbpbpbp");
    CheckEncode(0, 90, 8, "upbpbpbp");
    CheckEncode(0, -90, 7, "h000000");
    CheckEncode(180, 90, 6, "zzzzzz");
    CheckEncode(-180, -90, 5, "00000");
    CheckEncode(180, -90, 4, "pbpb");
    CheckEncode(110.1, 30.2, 5, "wmq7c");
    CheckEncode(110.1, 30.2, 1, "w");
    CheckEncode(116.453137, 39.936846, 11, "wx4g1yq8fz8");
}

TEST_F(GeoHashUtilTest, TestEncodeAbnormal)
{
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(0, 0, 0));
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(0, 0, 12));
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(-180.1, 0, 1));
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(180.1, 0, 1));
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(0, 90.1, 1));
    ASSERT_EQ((uint64_t)0, GeoHashUtil::Encode(0, -90.1, 1));
}
TEST_F(GeoHashUtilTest, TestDecodeAbnormal) {}

TEST_F(GeoHashUtilTest, TestGeoHashStrToUInt64)
{
    uint64_t hash = GeoHashUtil::GeoStrToHash("0bc0");
    std::string geoStr = GeoHashUtil::HashToGeoStr(hash);
    std::string expectStr = std::string("0bc0");
    ASSERT_EQ(expectStr, geoStr);
}

TEST_F(GeoHashUtilTest, TestGetSubGeoHashIds)
{
    uint64_t hash = GeoHashUtil::GeoStrToHash("0bc0");
    std::vector<uint64_t> subHashIds;
    GeoHashUtil::GetSubGeoHashIds(hash, subHashIds);
    ASSERT_EQ((size_t)32, subHashIds.size());
    const char* cellStrs = "0123456789bcdefghjkmnpqrstuvwxyz";
    for (size_t i = 0; i < strlen(cellStrs); i++) {
        std::string expectStr("0bc0");
        expectStr.append(1, cellStrs[i]);
        std::string actualStr = GeoHashUtil::HashToGeoStr(subHashIds[i]);
        ASSERT_EQ(expectStr, actualStr);
    }
}

TEST_F(GeoHashUtilTest, TestDistanceToLevel)
{
    ASSERT_EQ((int8_t)11, GeoHashUtil::DistanceToLevel(0));
    ASSERT_EQ((int8_t)11, GeoHashUtil::DistanceToLevel(0.00000000001));
    ASSERT_EQ((int8_t)11, GeoHashUtil::DistanceToLevel(0.149));
    ASSERT_EQ((int8_t)10, GeoHashUtil::DistanceToLevel(1.2));
    ASSERT_EQ((int8_t)5, GeoHashUtil::DistanceToLevel(5000));
    ASSERT_EQ((int8_t)1, GeoHashUtil::DistanceToLevel(40076000.0));
    ASSERT_EQ((int8_t)1, GeoHashUtil::DistanceToLevel(940076000.0));

    ASSERT_EQ((int8_t)11, GeoHashUtil::DistanceToLevel(GeoHashUtil::_levelDistErr[GeoHashUtil::MAX_LEVEL]));
    ASSERT_EQ((int8_t)1, GeoHashUtil::DistanceToLevel(GeoHashUtil::_levelDistErr[0]));
}
} // namespace indexlib::index
