#include "indexlib/index/common/field_format/spatial/ShapeIndexQueryStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Line.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class ShapeIndexQueryStrategyTest : public TESTBASE
{
public:
    ShapeIndexQueryStrategyTest() = default;
    ~ShapeIndexQueryStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void ExtractExpectedTerms(const std::string& geoHashStr, std::set<uint64_t>& terms,
                              const std::string& leafGeoHashStr)
    {
        terms.clear();
        std::vector<autil::StringView> geohashStrVec = autil::StringTokenizer::constTokenize(
            autil::StringView(geoHashStr), ",",
            autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        for (size_t i = 0; i < geohashStrVec.size(); i++) {
            uint64_t cellId = GeoHashUtil::GeoStrToHash(geohashStrVec[i].to_string());
            GeoHashUtil::SetLeafTag(cellId);
            terms.insert(cellId);
        }

        geohashStrVec = autil::StringTokenizer::constTokenize(autil::StringView(leafGeoHashStr), ",",
                                                              autil::StringTokenizer::TOKEN_TRIM |
                                                                  autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

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
AUTIL_LOG_SETUP(indexlib.index, ShapeIndexQueryStrategyTest);

void ShapeIndexQueryStrategyTest::setUp() {}

void ShapeIndexQueryStrategyTest::tearDown() {}

TEST_F(ShapeIndexQueryStrategyTest, testUsage)
{
    // test convert leaf cell and non leaf cell
    {
        ShapeIndexQueryStrategy strategy(1, 2, "", 0, 3000);
        auto shape = Line::FromString("-135.0001 -0.0001, -135.0001 45.0001,"
                                      "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                                      "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                                      "-105 0, -100 45, -95 0, -90 45");
        auto terms = strategy.CalculateTerms(shape);
        std::vector<std::string> words;
        for (auto term : terms) {
            words.push_back(std::to_string(term));
        }
        std::set<uint64_t> expectedTerms;
        ExtractExpectedTerms("2,3,6,8,9,b,c,d,f,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
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

} // namespace indexlib::index
