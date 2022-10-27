#include "indexlib/common/field_format/spatial/test/shape_index_query_strategy_unittest.h"
#include "indexlib/common/field_format/spatial/shape/shape_creator.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(spatial, ShapeIndexQueryStrategyTest);

ShapeIndexQueryStrategyTest::ShapeIndexQueryStrategyTest()
{
}

ShapeIndexQueryStrategyTest::~ShapeIndexQueryStrategyTest()
{
}

void ShapeIndexQueryStrategyTest::CaseSetUp()
{
}

void ShapeIndexQueryStrategyTest::CaseTearDown()
{
}

void ShapeIndexQueryStrategyTest::TestCalculateTerms()
{
    //test convert leaf cell and non leaf cell
    {
        ShapeIndexQueryStrategy strategy(1, 2, "", false);
        ShapePtr shape = ShapeCreator::Create("line",
                "-135.0001 -0.0001, -135.0001 45.0001,"
                "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                "-105 0, -100 45, -95 0, -90 45"); 
        vector<dictkey_t> terms;
        strategy.CalculateTerms(shape, terms);
        vector<string> words;
        for (size_t i = 0; i < terms.size(); i++)
        {
            words.push_back(StringUtil::toString(terms[i]));
        }
        std::set<uint64_t> expectedTerms;
        ExtractExpectedTerms(
                "2,3,6,8,9,b,c,d,f,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                "d5,dh,dj,dn,dp,f0",
                expectedTerms,
                "2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                "d5,dh,dj,dn,dp,f0");
        ASSERT_TRUE(CheckTerms(words, expectedTerms));
    }

}

void ShapeIndexQueryStrategyTest::ExtractExpectedTerms(
        const std::string& geoHashStr,
        std::set<uint64_t>& terms,
        const std::string& leafGeoHashStr)
{
    terms.clear();

    vector<ConstString> geohashStrVec = 
        autil::StringTokenizer::constTokenize(autil::ConstString(geoHashStr),
                ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < geohashStrVec.size(); i++)
    {
        uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].toString());
        GeoHashUtil::SetLeafTag(cellId);
        terms.insert(cellId);
    }

    geohashStrVec = 
        autil::StringTokenizer::constTokenize(autil::ConstString(leafGeoHashStr),
                ",",
                autil::StringTokenizer::TOKEN_TRIM |
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    for (size_t i = 0; i < geohashStrVec.size(); i++)
    {
        uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].toString());
        terms.insert(cellId);
    }
}

bool ShapeIndexQueryStrategyTest::CheckTerms(
        const std::vector<string>& terms,
        const std::set<uint64_t>& expectedTerms)
{
    if (terms.size() != expectedTerms.size())
    {
        IE_LOG(ERROR, "terms count not match: actual=[%lu], expected=[%lu]",
               terms.size(), expectedTerms.size());
        return false;
    }
    uint64_t word2Id;
    for (size_t i = 0; i < terms.size(); i++)
    {
        const std::string word = terms[i];
        if (!autil::StringUtil::strToUInt64(word.c_str(), word2Id))
        {
            IE_LOG(ERROR, "illegal uint64_t word: [%s]", word.c_str());
            return false;
        }
        if (expectedTerms.find(word2Id) == expectedTerms.end())
        {
            IE_LOG(ERROR, "not expected uint64_t word: [%lu]", word2Id);
            return false;
        }
    }
    return true;
}

IE_NAMESPACE_END(common);

