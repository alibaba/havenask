#include "sql/ops/scan/udf_to_query/PidvidContainUdfToQuery.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
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
class PidvidContainUdfToQueryTest : public TESTBASE {
public:
    PidvidContainUdfToQueryTest();
    ~PidvidContainUdfToQueryTest();

public:
    void setUp();
    void tearDown();
    void
    constructIndexInfos(const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs);

private:
    PidvidContainUdfToQuery _containUdfToQuery;
    UdfToQueryParam _param;
    map<string, IndexInfo> _indexInfoMap;
    suez::turing::IndexInfos _indexInfos;
};

PidvidContainUdfToQueryTest::PidvidContainUdfToQueryTest() {}

PidvidContainUdfToQueryTest::~PidvidContainUdfToQueryTest() {}

void PidvidContainUdfToQueryTest::setUp() {
    _indexInfoMap["pidvid"] = IndexInfo("pidvid", "string");
    _param.indexInfo = &_indexInfoMap;
    constructIndexInfos({{"pidvid", it_string}});
    _param.indexInfos = &_indexInfos;
}

void PidvidContainUdfToQueryTest::constructIndexInfos(
    const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs) {
    for (const auto &pair : indexs) {
        auto *index = new suez::turing::IndexInfo();
        index->setIndexName(pair.first.c_str());
        index->setIndexType(pair.second);
        _indexInfos.addIndexInfo(index);
    }
}

void PidvidContainUdfToQueryTest::tearDown() {}

TEST_F(PidvidContainUdfToQueryTest, testNotThisUdf) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"xxx", "params":["$pidvid"], "type":"UDF"})json";
    ASSERT_FALSE(_containUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "not pidvid_contain op", traces);
}

TEST_F(PidvidContainUdfToQueryTest, testParamSizeError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"pidvid_contain", "params":["$pidvid"], "type":"UDF"})json";

    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "param size 1 != 2", traces);
}

TEST_F(PidvidContainUdfToQueryTest, testNotColumn) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"pidvid_contain", "params":["pidvid","1:2|3:4"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[pidvid] not column", traces);
}

TEST_F(PidvidContainUdfToQueryTest, testIndex) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"pidvid_contain", "params":["$pidvid","1:2|3:4"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(0, "", traces);
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    const auto &terms = multiQuery->getTermArray();
    ASSERT_EQ(2, terms.size());
    EXPECT_EQ("1:2", terms[0]->getWord());
    EXPECT_EQ("3:4", terms[1]->getWord());
}

TEST_F(PidvidContainUdfToQueryTest, testEmpty) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"pidvid_contain", "params":["$pidvid",""], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "term size is empty", traces);
    ASSERT_TRUE(query == NULL);
}

} // namespace sql
