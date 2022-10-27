#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/test/location_index_query_strategy_unittest.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"
#include "indexlib/common/field_format/spatial/shape/shape_creator.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, LocationIndexQueryStrategyTest);

LocationIndexQueryStrategyTest::LocationIndexQueryStrategyTest()
{
}

LocationIndexQueryStrategyTest::~LocationIndexQueryStrategyTest()
{
}

void LocationIndexQueryStrategyTest::CaseSetUp()
{
}

void LocationIndexQueryStrategyTest::CaseTearDown()
{
}

void LocationIndexQueryStrategyTest::TestSimpleProcess()
{
    LocationIndexQueryStrategy strategy(1,8,"", true);
    vector<dictkey_t> queryTerms;
    queryTerms.clear();
    PointPtr point(new Point(0,0));
    strategy.CalculateTerms(point, queryTerms);
    ASSERT_EQ((size_t)1, queryTerms.size());
    uint64_t geoHashId = GeoHashUtil::Encode(0,0,8);
    ASSERT_EQ(geoHashId, queryTerms[0]);
}

void LocationIndexQueryStrategyTest::TestCircle()
{
    //according to geo_hash_util DistanceToLevel to calculate level
    InnerTestStrategy("circle", "0 0,50000", 5);
    InnerTestStrategy("circle", "0 0,70000", 5);
    InnerTestStrategy("circle", "0 0,1000", 7);
    InnerTestStrategy("circle", "0 0,500", 8);
    InnerTestStrategy("circle", "0 0,599", 8);
    InnerTestStrategy("circle", "0 0,100", 8);
    InnerTestStrategy("circle", "0 0,5009400.0", 2);
    InnerTestStrategy("circle", "0 0,1252300.0", 3);
    InnerTestStrategy("circle", "0 0,10", 8);
}

void LocationIndexQueryStrategyTest::InnerTestStrategy(const string& shapeName, 
        const string& shapeStr, const uint8_t expectDetailLevel)
{
    ShapePtr shape = ShapeCreator::Create(shapeName, shapeStr);
    ASSERT_TRUE(shape);
    vector<dictkey_t> queryTerms;
    LocationIndexQueryStrategy strategy(2,8,"", true);
    strategy.CalculateTerms(shape, queryTerms);
    //ASSERT_TRUE(maxTermCount >= queryTerms.size());
    //cout << queryTerms.size() << endl;
    for (size_t i = 0; i < queryTerms.size(); i++)
    {
        uint64_t hashId = queryTerms[i];
        ASSERT_TRUE(2 <= GeoHashUtil::GetLevelOfHashId(hashId));
        ASSERT_TRUE(expectDetailLevel >= GeoHashUtil::GetLevelOfHashId(hashId))
            << (uint64_t)expectDetailLevel;
    }
}

void LocationIndexQueryStrategyTest::TestCalculateTerms()
{
    {
        LocationIndexQueryStrategy strategy(1, 11 , "", true);
        ShapePtr shape = ShapeCreator::Create("polygon",
                "-135.0001 -0.0001,"
                "-135.0001 45.0001,"
                "-89.9999 45.0001,"
                "-89.9999 -0.0001,"
                "-135.0001 -0.0001"); 
        vector<dictkey_t> terms;
        strategy.CalculateTerms(shape, terms);
        vector<string> words;
        for (size_t i = 0; i < terms.size(); i++)
        {
            words.push_back(StringUtil::toString(terms[i]));
        }
        std::set<uint64_t> expectedTerms;
        ExtractExpectedTerms("9,2,3,6,8,b,c,d,f", expectedTerms);
        ASSERT_TRUE(CheckTerms(words, expectedTerms));
    }
}

void LocationIndexQueryStrategyTest::ExtractExpectedTerms(
        const std::string& geoHashStr,
        std::set<uint64_t>& terms)
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
        terms.insert(cellId);
    }
}

bool LocationIndexQueryStrategyTest::CheckTerms(
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

