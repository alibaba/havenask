#include "sql/common/KvPairParser.h"

#include <cstddef>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "unittest/unittest.h"

using namespace std;

namespace sql {

class KvPairParserTest : public TESTBASE {};

TEST_F(KvPairParserTest, testGetNextTerm) {
    string input = "abc\\:def:aa";
    size_t start = 0;
    string output = KvPairParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc:def"), output);
    ASSERT_EQ(size_t(9), start);

    input = ":abc\\:def:aa";
    start = 0;
    output = KvPairParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string(""), output);
    ASSERT_EQ(size_t(1), start);

    input = "\\:abc\\:def:aa";
    start = 0;
    output = KvPairParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string(":abc:def"), output);
    ASSERT_EQ(size_t(11), start);

    input = "abc:";
    start = 0;
    output = KvPairParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc"), output);
    ASSERT_EQ(size_t(4), start);

    input = "abc\\:";
    start = 0;
    output = KvPairParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc:"), output);
    ASSERT_EQ(size_t(5), start);
}

TEST_F(KvPairParserTest, testParseKVPair) {
    unordered_map<string, string> kvPair;
    string oriStr = "key:value;key2:value2;key\\:aa\\;:value\\:bb\\;;";
    KvPairParser::parse(oriStr, ';', ':', kvPair);
    ASSERT_EQ(size_t(3), kvPair.size());
    ASSERT_EQ(string("value"), kvPair["key"]);
    ASSERT_EQ(string("value2"), kvPair["key2"]);
    ASSERT_EQ(string("value:bb;"), kvPair["key:aa;"]);
}

TEST_F(KvPairParserTest, testParseKVPair_Map) {
    map<string, string> kvPair;
    string oriStr = "key:value;key2:value2;key\\:aa\\;:value\\:bb\\;;";
    KvPairParser::parse(oriStr, ';', ':', kvPair);
    ASSERT_EQ(size_t(3), kvPair.size());
    ASSERT_EQ(string("value"), kvPair["key"]);
    ASSERT_EQ(string("value2"), kvPair["key2"]);
    ASSERT_EQ(string("value:bb;"), kvPair["key:aa;"]);
}

TEST_F(KvPairParserTest, testParseKVPair_DynamicParamsWithEmoji) {
    map<string, string> kvPair;
    string oriStr
        = R"raw(databaseName:phone;timeout:10000;dynamic_params:[["'\\ud83d\\udc1f\\ud83d\\ude0a'"]];formatType:string)raw";
    KvPairParser::parse(oriStr, ';', ':', kvPair);
    auto it = kvPair.find("dynamic_params");
    ASSERT_NE(kvPair.end(), it);
    ASSERT_EQ(R"raw([["'\ud83d\udc1f\ud83d\ude0a'"]])raw", it->second);
}

} // namespace sql
