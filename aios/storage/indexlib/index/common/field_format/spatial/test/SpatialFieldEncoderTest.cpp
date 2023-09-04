#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"

#include "autil/StringTokenizer.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class SpatialFieldEncoderTest : public TESTBASE
{
public:
    SpatialFieldEncoderTest() = default;
    ~SpatialFieldEncoderTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void ExtractExpectedTerms(const std::string& notLeafGeoHashStr, const std::string& leafGeoHashStr,
                              std::set<dictkey_t>& terms)
    {
        terms.clear();

        std::vector<autil::StringView> geohashStrVec = autil::StringTokenizer::constTokenize(
            autil::StringView(notLeafGeoHashStr), ",",
            autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        for (size_t i = 0; i < geohashStrVec.size(); i++) {
            uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].to_string());
            terms.insert(cellId);
        }

        geohashStrVec = autil::StringTokenizer::constTokenize(autil::StringView(leafGeoHashStr), ",",
                                                              autil::StringTokenizer::TOKEN_TRIM |
                                                                  autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        for (size_t i = 0; i < geohashStrVec.size(); i++) {
            uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].to_string());
            GeoHashUtil::SetLeafTag(cellId);
            terms.insert(cellId);
        }
    }

    bool CheckTerms(const std::vector<dictkey_t>& terms, const std::set<dictkey_t>& expectedTerms)
    {
        if (terms.size() != expectedTerms.size()) {
            AUTIL_LOG(ERROR, "terms count not match: actual=[%lu], expected=[%lu]", terms.size(), expectedTerms.size());
            return false;
        }
        for (auto term : terms) {
            if (expectedTerms.find(term) == expectedTerms.end()) {
                AUTIL_LOG(ERROR, "not expected dictkey_t word: [%lu]", term);
                return false;
            }
        }
        return true;
    }

private:
    AUTIL_LOG_DECLARE();
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> _indexConfigs;
};

AUTIL_LOG_SETUP(indexlib.index, SpatialFieldEncoderTest);

void SpatialFieldEncoderTest::setUp()
{
    fieldid_t fieldId = 0;
    auto MakeSpatialIndexConfig =
        [&fieldId](const std::string& indexName, const std::string& fieldName,
                   FieldType fieldType) -> std::shared_ptr<indexlibv2::config::SpatialIndexConfig> {
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>(fieldName, fieldType, true);
        fieldConfig->SetFieldId(fieldId);
        fieldId += 2;
        auto indexConfig = std::make_shared<indexlibv2::config::SpatialIndexConfig>(indexName, it_spatial);
        auto status = indexConfig->SetFieldConfig(fieldConfig);
        assert(status.IsOK());
        return indexConfig;
    };
    _indexConfigs = {
        MakeSpatialIndexConfig("spatial_index", "coordinate", FieldType::ft_location),
        MakeSpatialIndexConfig("line_index", "line", FieldType::ft_line),
        MakeSpatialIndexConfig("polygon_index", "polygon", FieldType::ft_polygon),
    };
}

void SpatialFieldEncoderTest::tearDown() {}

TEST_F(SpatialFieldEncoderTest, testInit)
{
    SpatialFieldEncoder encoder(_indexConfigs);
    ASSERT_EQ(1, encoder.TEST_GetTopLevel(0));
    ASSERT_EQ(11, encoder.TEST_GetDetailLevel(0));
    ASSERT_EQ(indexlibv2::config::SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY, encoder.TEST_GetDistanceLoss(0));
}

TEST_F(SpatialFieldEncoderTest, TestIsSpatialIndexField)
{
    SpatialFieldEncoder encoder(_indexConfigs);
    ASSERT_TRUE(encoder.IsSpatialIndexField(0));
    ASSERT_FALSE(encoder.IsSpatialIndexField(1));
    ASSERT_TRUE(encoder.IsSpatialIndexField(2));
    ASSERT_FALSE(encoder.IsSpatialIndexField(3));
    ASSERT_TRUE(encoder.IsSpatialIndexField(4));
    ASSERT_FALSE(encoder.IsSpatialIndexField(5));
}

TEST_F(SpatialFieldEncoderTest, TestEncodeNonSpatialIndexField)
{
    SpatialFieldEncoder encoder(_indexConfigs);
    std::vector<dictkey_t> dictKeys = {1234}; // junk, need be clear
    fieldid_t nonSpatialFieldId = 1;
    encoder.Encode(nonSpatialFieldId, "10 10", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
}

TEST_F(SpatialFieldEncoderTest, TestEncodePoint)
{
    auto spatialIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SpatialIndexConfig>(_indexConfigs[0]);
    spatialIndexConfig->SetMaxSearchDist(5000);
    spatialIndexConfig->SetMaxDistError(0.05);

    SpatialFieldEncoder encoder(_indexConfigs);
    auto fieldId = spatialIndexConfig->GetFieldConfig()->GetFieldId();

    std::vector<dictkey_t> dictKeys;
    encoder.Encode(fieldId, "0 0", dictKeys);
    ASSERT_EQ((size_t)7, dictKeys.size());
    // depend on GeoHashUtil::Encode
    uint64_t baseDictKey = 0xc000000000000000L;
    ASSERT_THAT(dictKeys,
                testing::UnorderedElementsAre(baseDictKey + 10, baseDictKey + 12, baseDictKey + 14, baseDictKey + 16,
                                              baseDictKey + 18, baseDictKey + 20, baseDictKey + 22));

    // abnormal
    encoder.Encode(fieldId, "", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
    encoder.Encode(fieldId, "invalid", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
}

TEST_F(SpatialFieldEncoderTest, TestEncodeShape)
{
    {
        auto lineIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SpatialIndexConfig>(_indexConfigs[1]);
        lineIndexConfig->SetMaxSearchDist(5009400.0);
        lineIndexConfig->SetMaxDistError(1252300.0);

        SpatialFieldEncoder encoder(_indexConfigs);
        std::vector<dictkey_t> dictKeys;
        auto fieldId = lineIndexConfig->GetFieldConfig()->GetFieldId();

        encoder.Encode(fieldId,
                       "-135.0001 -0.0001, -135.0001 45.0001,"
                       "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                       "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                       "-105 0, -100 45, -95 0, -90 45",
                       dictKeys);

        ASSERT_EQ((size_t)69, dictKeys.size());

        std::set<dictkey_t> expectedTerms;
        ExtractExpectedTerms("2,3,6,8,9,b,c,d,f",
                             "2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                             "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                             "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                             "d5,dh,dj,dn,dp,f0",
                             expectedTerms);

        ASSERT_TRUE(CheckTerms(dictKeys, expectedTerms));
    }

    {
        auto polygonIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SpatialIndexConfig>(_indexConfigs[2]);
        polygonIndexConfig->SetMaxSearchDist(5009400.0);
        polygonIndexConfig->SetMaxDistError(1252300.0);

        SpatialFieldEncoder encoder(_indexConfigs);
        std::vector<dictkey_t> dictKeys;
        auto fieldId = polygonIndexConfig->GetFieldConfig()->GetFieldId();

        encoder.Encode(fieldId,
                       "-135.0001 -0.0001,"
                       "-135.0001 45.0001,"
                       "-89.9999 45.0001,"
                       "-89.9999 -0.0001,"
                       "-135.0001 -0.0001",
                       dictKeys);

        ASSERT_EQ((size_t)37, dictKeys.size());

        std::set<dictkey_t> expectedTerms;
        ExtractExpectedTerms("2,3,6,8,b,c,d,f",
                             "9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                             "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                             expectedTerms);

        ASSERT_TRUE(CheckTerms(dictKeys, expectedTerms));
    }
}

} // namespace indexlib::index
