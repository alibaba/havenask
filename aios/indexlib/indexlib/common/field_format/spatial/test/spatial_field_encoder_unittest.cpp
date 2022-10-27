#include "indexlib/common/field_format/spatial/test/spatial_field_encoder_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SpatialFieldEncoderTest);

SpatialFieldEncoderTest::SpatialFieldEncoderTest()
{
}

SpatialFieldEncoderTest::~SpatialFieldEncoderTest()
{
}

void SpatialFieldEncoderTest::CaseSetUp()
{
    string field = "pk:string:pk;coordinate:location;string:string;"
                   "line:line;polygon:polygon";
    string index = "pk:primarykey64:pk;spatial_index:spatial:coordinate;"
                   "string_index:string:string;"
                   "line_index:spatial:line;"
                   "polygon_index:spatial:polygon";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
}

void SpatialFieldEncoderTest::CaseTearDown()
{
}

void SpatialFieldEncoderTest::TestInit()
{
    SpatialFieldEncoder encoder(mSchema);
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
    fieldid_t fieldId = fieldSchema->GetFieldId("coordinate");
    ASSERT_EQ((int8_t)1, encoder.GetTopLevel(fieldId));
    ASSERT_EQ((int8_t)11, encoder.GetDetailLevel(fieldId));
    ASSERT_EQ(SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY,
              encoder.mDistanceLoss[fieldId]);
}

void SpatialFieldEncoderTest::TestIsSpatialIndexField()
{
    SpatialFieldEncoder encoder(mSchema);
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
    ASSERT_FALSE(encoder.IsSpatialIndexField(fieldSchema->GetFieldId("pk")));
    ASSERT_TRUE(encoder.IsSpatialIndexField(fieldSchema->GetFieldId("coordinate")));
    ASSERT_FALSE(encoder.IsSpatialIndexField(fieldSchema->GetFieldId("string")));
}

void SpatialFieldEncoderTest::TestEncodeNonSpatialIndexField()
{
    SpatialFieldEncoder encoder(mSchema);
    vector<dictkey_t> dictKeys;
    dictKeys.push_back(1234); // junk, need be clear
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
    fieldid_t nonSpatialFieldId = fieldSchema->GetFieldId("pk");
    encoder.Encode(nonSpatialFieldId, "10 10", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
}

void SpatialFieldEncoderTest::TestEncodePoint()
{
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(
            SpatialIndexConfig, indexSchema->GetIndexConfig("spatial_index"));
    spatialIndexConfig->SetMaxSearchDist(5000);
    spatialIndexConfig->SetMaxDistError(0.05);

    SpatialFieldEncoder encoder(mSchema);
    vector<dictkey_t> dictKeys;
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
    fieldid_t fieldId = fieldSchema->GetFieldId("coordinate");

    encoder.Encode(fieldId, "0 0", dictKeys);
    ASSERT_EQ((size_t)7, dictKeys.size());
    // depend on GeoHashUtil::Encode
    uint64_t baseDictKey = 0xc000000000000000L;
    ASSERT_THAT(dictKeys, UnorderedElementsAre(
                    baseDictKey + 10, baseDictKey + 12, baseDictKey + 14,
                    baseDictKey + 16, baseDictKey + 18, baseDictKey + 20,
                    baseDictKey + 22));

    // abnormal
    encoder.Encode(fieldId, "", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
    encoder.Encode(fieldId, "invalid", dictKeys);
    ASSERT_EQ((size_t)0, dictKeys.size());
}

void SpatialFieldEncoderTest::TestEncodeShape()
{
    {
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        SpatialIndexConfigPtr lineIndexConfig = DYNAMIC_POINTER_CAST(
                SpatialIndexConfig, indexSchema->GetIndexConfig("line_index"));
        lineIndexConfig->SetMaxSearchDist(5009400.0);
        lineIndexConfig->SetMaxDistError(1252300.0);

        SpatialFieldEncoder encoder(mSchema);
        vector<dictkey_t> dictKeys;
        const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
        fieldid_t fieldId = fieldSchema->GetFieldId("line");

        encoder.Encode(
                fieldId,
                "-135.0001 -0.0001, -135.0001 45.0001,"
                "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                "-105 0, -100 45, -95 0, -90 45",
                dictKeys);

        ASSERT_EQ((size_t)69, dictKeys.size());

        std::set<dictkey_t> expectedTerms;
        ExtractExpectedTerms(
                "2,3,6,8,9,b,c,d,f",
                "2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                "d5,dh,dj,dn,dp,f0",
                expectedTerms);

        ASSERT_TRUE(CheckTerms(dictKeys, expectedTerms));
    }

    {
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        SpatialIndexConfigPtr polygonIndexConfig = DYNAMIC_POINTER_CAST(
                SpatialIndexConfig, indexSchema->GetIndexConfig("polygon_index"));
        polygonIndexConfig->SetMaxSearchDist(5009400.0);
        polygonIndexConfig->SetMaxDistError(1252300.0);

        SpatialFieldEncoder encoder(mSchema);
        vector<dictkey_t> dictKeys;
        const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
        fieldid_t fieldId = fieldSchema->GetFieldId("polygon");

        encoder.Encode(
                fieldId,
                "-135.0001 -0.0001,"
                "-135.0001 45.0001,"
                "-89.9999 45.0001,"
                "-89.9999 -0.0001,"
                "-135.0001 -0.0001",
                dictKeys);

        ASSERT_EQ((size_t)37, dictKeys.size());

        std::set<dictkey_t> expectedTerms;
        ExtractExpectedTerms(
                "2,3,6,8,b,c,d,f",
                "9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                expectedTerms);

        ASSERT_TRUE(CheckTerms(dictKeys, expectedTerms));
    }
}

void SpatialFieldEncoderTest::ExtractExpectedTerms(
        const std::string& notLeafGeoHashStr,
        const std::string& leafGeoHashStr,
        std::set<dictkey_t>& terms)
{
    terms.clear();

    vector<ConstString> geohashStrVec = 
        autil::StringTokenizer::constTokenize(
                autil::ConstString(notLeafGeoHashStr),
                ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < geohashStrVec.size(); i++)
    {
        uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].toString());
        terms.insert(cellId);
    }

    geohashStrVec = 
        autil::StringTokenizer::constTokenize(
                autil::ConstString(leafGeoHashStr),
                ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < geohashStrVec.size(); i++)
    {
        uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].toString());
        GeoHashUtil::SetLeafTag(cellId);
        terms.insert(cellId);
    }
}

bool SpatialFieldEncoderTest::CheckTerms(
        const std::vector<dictkey_t>& terms,
        const std::set<dictkey_t>& expectedTerms)
{
    if (terms.size() != expectedTerms.size())
    {
        IE_LOG(ERROR, "terms count not match: actual=[%lu], expected=[%lu]",
               terms.size(), expectedTerms.size());
        return false;
    }
    for (size_t i = 0; i < terms.size(); i++)
    {
        if (expectedTerms.find(terms[i]) == expectedTerms.end())
        {
            IE_LOG(ERROR, "not expected dictkey_t word: [%lu]", terms[i]);
            return false;
        }
    }
    return true;
}

IE_NAMESPACE_END(common);

