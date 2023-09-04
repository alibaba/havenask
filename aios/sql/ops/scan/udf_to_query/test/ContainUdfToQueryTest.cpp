#include "sql/ops/scan/udf_to_query/ContainUdfToQuery.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
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
class ContainUdfToQueryTest : public TESTBASE {
public:
    ContainUdfToQueryTest();
    ~ContainUdfToQueryTest();

public:
    void setUp();
    void tearDown();
    void
    constructIndexInfos(const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs);

private:
    ContainUdfToQuery _containUdfToQuery;
    UdfToQueryParam _param;
    map<string, IndexInfo> _indexInfoMap;
    suez::turing::IndexInfos _indexInfos;
};

ContainUdfToQueryTest::ContainUdfToQueryTest() {}

ContainUdfToQueryTest::~ContainUdfToQueryTest() {}

void ContainUdfToQueryTest::setUp() {
    _indexInfoMap["name"] = IndexInfo("name", "string");
    _indexInfoMap["index_2"] = IndexInfo("index_2", "string");
    _indexInfoMap["attr2"] = IndexInfo("attr2", "number");
    _param.indexInfo = &_indexInfoMap;
    constructIndexInfos({{"name", it_string}, {"index_2", it_string}, {"attr2", it_number_int16}});
    _param.indexInfos = &_indexInfos;
}

void ContainUdfToQueryTest::constructIndexInfos(
    const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs) {
    for (const auto &pair : indexs) {
        auto *index = new suez::turing::IndexInfo();
        index->setIndexName(pair.first.c_str());
        index->setIndexType(pair.second);
        _indexInfos.addIndexInfo(index);
    }
}

void ContainUdfToQueryTest::tearDown() {}

TEST_F(ContainUdfToQueryTest, testNotContain) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"xxx", "params":["bizName"], "type":"UDF"})json";
    ASSERT_FALSE(_containUdfToQuery.toQuery(simpleDoc, _param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "not contain or ha_in op", traces);
}

TEST_F(ContainUdfToQueryTest, testParamSizeError) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = R"json({"op":"contain", "params":["bizName"], "type":"UDF"})json";

    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "param size 1 != 2 or 3", traces);
}

TEST_F(ContainUdfToQueryTest, testNotColumn) {
    navi::NaviLoggerProvider provider("DEBUG");
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"name\", \"aa|bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "[name] not column", traces);
}

TEST_F(ContainUdfToQueryTest, testErrorIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"$name\", \"aa|bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testErrorSep) {
    autil::SimpleDocument simpleDoc;
    string condStr
        = "{\"op\":\"contain\", \"params\":[\"$name\", \"aa#bb\", \"#\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testNumberIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"$attr2\", \"1|2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testNumberIndex_SingleValue) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"$attr2\", \"2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("NumberQuery", query->getQueryName());
    const NumberQuery *numberQuery = dynamic_cast<NumberQuery *>(query.get());
    ASSERT_TRUE(numberQuery != NULL);
    auto &numberTerm = numberQuery->getTerm();
    ASSERT_EQ(2, numberTerm.getLeftNumber());
    ASSERT_EQ(2, numberTerm.getRightNumber());
}

TEST_F(ContainUdfToQueryTest, testNumberIndexError) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"$attr2\", \"1|a2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(ContainUdfToQueryTest, testNoIndex) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"$attr1\", \"1|2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(ContainUdfToQueryTest, testNameIndex_hain) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"ha_in\", \"params\":[\"$name\", \"aa|bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testErrorSep_hain) {
    autil::SimpleDocument simpleDoc;
    string condStr
        = "{\"op\":\"ha_in\", \"params\":[\"$name\", \"aa#bb\", \"#\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testNumberIndex_hain) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"ha_in\", \"params\":[\"$attr2\", \"1|2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("MultiTermQuery", query->getQueryName());
    MultiTermQuery *multiQuery = dynamic_cast<MultiTermQuery *>(query.get());
    ASSERT_TRUE(multiQuery != NULL);
    ASSERT_EQ(2, multiQuery->getTermArray().size());
}

TEST_F(ContainUdfToQueryTest, testNumberIndexError_hain) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"ha_in\", \"params\":[\"$attr2\", \"1|a2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(ContainUdfToQueryTest, testNoIndex_hain) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"1|2\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_containUdfToQuery.toQuery(simpleDoc, _param));
    ASSERT_TRUE(query == NULL);
}

} // namespace sql
