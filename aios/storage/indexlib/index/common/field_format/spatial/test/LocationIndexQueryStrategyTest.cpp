#include "indexlib/index/common/field_format/spatial/LocationIndexQueryStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Circle.h"
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class LocationIndexQueryStrategyTest : public TESTBASE
{
public:
    LocationIndexQueryStrategyTest() = default;
    ~LocationIndexQueryStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void ExtractExpectedTerms(const std::string& geoHashStr, std::set<uint64_t>& terms)
    {
        terms.clear();
        std::vector<autil::StringView> geohashStrVec = autil::StringTokenizer::constTokenize(
            autil::StringView(geoHashStr), ",",
            autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        for (size_t i = 0; i < geohashStrVec.size(); i++) {
            uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].to_string());
            terms.insert(cellId);
        }
    }

    bool CheckTerms(const std::vector<std::string>& terms, const std::set<uint64_t>& expectedTerms)
    {
        if (terms.size() != expectedTerms.size()) {
            AUTIL_LOG(ERROR, "terms count not match: actual=[%lu], expected=[%lu]", terms.size(), expectedTerms.size());
            return false;
        }
        uint64_t word2Id;
        for (size_t i = 0; i < terms.size(); i++) {
            const std::string word = terms[i];
            if (!autil::StringUtil::strToUInt64(word.c_str(), word2Id)) {
                AUTIL_LOG(ERROR, "illegal uint64_t word: [%s]", word.c_str());
                return false;
            }
            if (expectedTerms.find(word2Id) == expectedTerms.end()) {
                AUTIL_LOG(ERROR, "not expected uint64_t word: [%lu]", word2Id);
                return false;
            }
        }
        return true;
    }

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.index, LocationIndexQueryStrategyTest);

void LocationIndexQueryStrategyTest::setUp() {}

void LocationIndexQueryStrategyTest::tearDown() {}

TEST_F(LocationIndexQueryStrategyTest, testUsage)
{
    LocationIndexQueryStrategy strategy(1, 8, "", 1.0, 3000);
    auto queryTerms = strategy.CalculateTerms(std::make_shared<Point>(0, 0));
    std::vector<dictkey_t> expected = {GeoHashUtil::Encode(0, 0, 8)};
    ASSERT_EQ(expected, queryTerms);
}

TEST_F(LocationIndexQueryStrategyTest, TestCircle)
{
    auto InnerTest = [](const std::string& shapeStr, uint8_t expectDetailLevel) {
        auto shape = Circle::FromString(shapeStr);
        ASSERT_TRUE(shape);
        LocationIndexQueryStrategy strategy(2, 8, "", 1.0, 3000);
        auto queryTerms = strategy.CalculateTerms(shape);
        for (auto queryTerm : queryTerms) {
            ASSERT_TRUE(2 <= GeoHashUtil::GetLevelOfHashId(queryTerm));
            ASSERT_TRUE(expectDetailLevel >= GeoHashUtil::GetLevelOfHashId(queryTerm)) << (uint64_t)expectDetailLevel;
        }
    };
    InnerTest("0 0,50000", 5);
    InnerTest("0 0,70000", 5);
    InnerTest("0 0,1000", 7);
    InnerTest("0 0,500", 8);
    InnerTest("0 0,599", 8);
    InnerTest("0 0,100", 8);
    InnerTest("0 0,5009400.0", 2);
    InnerTest("0 0,1252300.0", 3);
    InnerTest("0 0,10", 8);
}

TEST_F(LocationIndexQueryStrategyTest, TestCalculateTerms)
{
    LocationIndexQueryStrategy strategy(1, 11, "", 1.0, 3000);
    auto polygon = Polygon::FromString("-135.0001 -0.0001,"
                                       "-135.0001 45.0001,"
                                       "-89.9999 45.0001,"
                                       "-89.9999 -0.0001,"
                                       "-135.0001 -0.0001");
    auto terms = strategy.CalculateTerms(polygon);
    std::vector<std::string> words;
    for (auto term : terms) {
        words.push_back(std::to_string(term));
    }
    std::set<uint64_t> expectedTerms;
    ExtractExpectedTerms("9,2,3,6,8,b,c,d,f", expectedTerms);
    ASSERT_TRUE(CheckTerms(words, expectedTerms));
}

} // namespace indexlib::index
