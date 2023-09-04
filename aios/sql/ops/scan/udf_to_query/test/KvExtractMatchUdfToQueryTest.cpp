#include "sql/ops/scan/udf_to_query/KvExtractMatchUdfToQuery.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/Query.h"
#include "ha3/turing/common/ModelConfig.h"
#include "indexlib/index/common/Types.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::turing;

namespace sql {
class KvExtractMatchUdfToQueryTest : public TESTBASE {
public:
    KvExtractMatchUdfToQueryTest();
    ~KvExtractMatchUdfToQueryTest();

public:
    void setUp();
    void tearDown();
    void
    constructIndexInfos(const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs);

private:
    KvExtractMatchUdfToQuery _kvExtractMatchUdfToQuery;
    UdfToQueryParam _param;
    map<string, IndexInfo> _indexInfoMap;
    suez::turing::IndexInfos _indexInfos;
};

KvExtractMatchUdfToQueryTest::KvExtractMatchUdfToQueryTest() {}

KvExtractMatchUdfToQueryTest::~KvExtractMatchUdfToQueryTest() {}

void KvExtractMatchUdfToQueryTest::setUp() {
    constructIndexInfos({{"attributes_rootCat", it_number_int64},
                         {"attributes_shopName", it_string},
                         {"attributes_keys", it_string}});
    _param.indexInfos = &_indexInfos;
}

void KvExtractMatchUdfToQueryTest::constructIndexInfos(
    const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs) {
    for (const auto &pair : indexs) {
        auto *index = new suez::turing::IndexInfo();
        index->setIndexName(pair.first.c_str());
        index->setIndexType(pair.second);
        _indexInfos.addIndexInfo(index);
    }
}

void KvExtractMatchUdfToQueryTest::tearDown() {}

TEST_F(KvExtractMatchUdfToQueryTest, testParamsSizeError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"kv_extract_match", "params":["1"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "param size 1 != 3 or 4", traces);
}

TEST_F(KvExtractMatchUdfToQueryTest, testParam1NotColumn) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["not_exist", "2", "3"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[not_exist] not column", traces);
}

TEST_F(KvExtractMatchUdfToQueryTest, testKeyMatch_IndexNotFound) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$not_exist", "", "rootCat"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[not_exist_keys] not index", traces);
}

TEST_F(KvExtractMatchUdfToQueryTest, testKeyMatch) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$attributes", "", "rootCat"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != nullptr);
    ASSERT_EQ("TermQuery$1:[Term:[attributes_keys||rootCat|100|]]", query->toString());
}

TEST_F(KvExtractMatchUdfToQueryTest, testKeyValueMatch_indexNotFound) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$not_exist", "", "rootCat", "1"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[not_exist_rootCat] not index", traces);
}

TEST_F(KvExtractMatchUdfToQueryTest, testKeyValueMatch) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$attributes", "", "rootCat", "1"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != nullptr);
    ASSERT_EQ("NumberQuery$1:[NumberTerm: [attributes_rootCat,[1, 1||1|100|]]", query->toString());
}

TEST_F(KvExtractMatchUdfToQueryTest, testKeyValueMatchArray) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$attributes", "", "rootCat", "1|2"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != nullptr);
    ASSERT_EQ("MultiTermQuery(OR)$1:[NumberTerm: [attributes_rootCat,[1, 1||1|100|], NumberTerm: "
              "[attributes_rootCat,[2, 2||2|100|], ]",
              query->toString());
}

TEST_F(KvExtractMatchUdfToQueryTest, testSep) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"kv_extract_match", "params":["$attributes", "#", "rootCat", "1#2"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_kvExtractMatchUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != nullptr);
    ASSERT_EQ("MultiTermQuery(OR)$1:[NumberTerm: [attributes_rootCat,[1, 1||1|100|], NumberTerm: "
              "[attributes_rootCat,[2, 2||2|100|], ]",
              query->toString());
}

} // namespace sql
