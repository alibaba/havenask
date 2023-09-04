#include "sql/ops/scan/udf_to_query/InUdfToQuery.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/Query.h"
#include "ha3/turing/common/ModelConfig.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::turing;

namespace sql {
class InUdfToQueryTest : public TESTBASE {
public:
    InUdfToQueryTest();
    ~InUdfToQueryTest();

public:
    void setUp();
    void tearDown();

private:
    InUdfToQuery _inUdfToQuery;
    UdfToQueryParam _param;
    map<string, IndexInfo> _indexInfoMap;
};

InUdfToQueryTest::InUdfToQueryTest() {}

InUdfToQueryTest::~InUdfToQueryTest() {}

void InUdfToQueryTest::setUp() {
    _indexInfoMap["name"] = IndexInfo("name", "string");
    _indexInfoMap["title"] = IndexInfo("name", "text");
    _indexInfoMap["index_2"] = IndexInfo("index_2", "string");
    _indexInfoMap["attr2"] = IndexInfo("attr2", "number");
    _param.indexInfo = &_indexInfoMap;
}

void InUdfToQueryTest::tearDown() {}

TEST_F(InUdfToQueryTest, testParamSizeError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"IN", "params":["bizName"], "type":"UDF"})json";

    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "param size 1 < 2", traces);
}

TEST_F(InUdfToQueryTest, testNotColumn) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"name\", \"aa|bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[name] not column", traces);
}

TEST_F(InUdfToQueryTest, testNotIndex) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$a\", \"aa|bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[a] not index", traces);
}

TEST_F(InUdfToQueryTest, testNumberIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$attr2\", 1],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(1, multiQuery->getTermArray().size());
}

TEST_F(InUdfToQueryTest, testNumberIndexList) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$attr2\", 1, 2],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(InUdfToQueryTest, testNumberIndexParamError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$attr2\", \"aa\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse param[1]:`\"aa\"` to number failed", traces);
}

TEST_F(InUdfToQueryTest, testStringIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$name\", \"a\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(1, multiQuery->getTermArray().size());
}

TEST_F(InUdfToQueryTest, testStringIndexList) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$name\", \"a\", \"b\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(InUdfToQueryTest, testStringIndexParamError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$name\", 1],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse param[1]:`1` to string failed", traces);
}

TEST_F(InUdfToQueryTest, testNotValidIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"IN\", \"params\":[\"$title\", \"aa\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_inUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
}

} // namespace sql
