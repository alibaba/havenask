#include "sql/ops/scan/Ha3ScanConditionVisitor.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/common/Query.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "ha3/turing/common/ModelConfig.h"
#include "matchdoc/ValueType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/ops/condition/CaseExpression.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "sql/ops/scan/test/ScanConditionTestUtil.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "unittest/unittest.h"

using namespace suez::turing;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;
using namespace isearch::turing;

namespace sql {

class Ha3ScanConditionVisitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    Ha3ScanConditionVisitorPtr createVisitor();

private:
    ScanConditionTestUtil _scanConditionTestUtil;
};

void Ha3ScanConditionVisitorTest::setUp() {
    _scanConditionTestUtil.init(GET_TEMPLATE_DATA_PATH(), GET_TEST_DATA_PATH());
}

void Ha3ScanConditionVisitorTest::tearDown() {}

Ha3ScanConditionVisitorPtr Ha3ScanConditionVisitorTest::createVisitor() {
    Ha3ScanConditionVisitorPtr vistor(new Ha3ScanConditionVisitor(_scanConditionTestUtil._param));
    return vistor;
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitOrCondition) {
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"<>\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"<>\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string condStr4 = "{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr3 + "," + condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ("((AndQuery:[TermQuery$1:[Term:[index_2||aa|100|]], "
                  "TermQuery$1:[Term:[index_2||bb|100|]], "
                  "] AND (attr1!=1)) OR TermQuery$1:[Term:[index_2||dd|100|]])",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }

    { // leaf has error, name1 not exist
        ConditionParser parser;
        string condStr1
            = "{\"op\":\"QUERY\", \"params\":[\"name1\", \"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // leaf has error, attr not cloumn
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // attr1 != 1 or name=true
        ConditionParser parser;
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"OR\",\"params\":[\"$brand\",{\"op\":\">\",\"params\":[\"$"
                         "price\",1],\"type\":\"OTHER\"}],\"type\":\"OTHER\"}";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitAndCondition) {
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string condStr4 = "{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr3 + "," + condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ("(attr1!=1)", visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(
            "AndQuery:[AndQuery:[TermQuery$1:[Term:[index_2||aa|100|]], "
            "TermQuery$1:[Term:[index_2||bb|100|]], ], TermQuery$1:[Term:[index_2||dd|100|]], ]",
            visitor->getQuery()->toString());
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    { // leaf has error, name1 not exist
        ConditionParser parser;
        string condStr1
            = "{\"op\":\"QUERY\", \"params\":[\"name1\", \"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // leaf has error, attr not cloumn
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // attr1 != 1 and name=true
        ConditionParser parser;
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"AND\",\"params\":[\"$brand\",{\"op\":\">\",\"params\":[\"$"
                         "price\",1],\"type\":\"OTHER\"}],\"type\":\"OTHER\"}";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    {
        ConditionParser parser;
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"({"op":"AND","params":[{"op":"=","params":["$attr1",0],"type":"OTHER"},{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}], "type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitNotCondition) {
    { // not query
        ConditionParser parser;
        string condStr = "{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        EXPECT_EQ("not (TermQuery$1:[Term:[index_2||aa|100|]])",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    { // not expr
        ConditionParser parser;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not ((attr1=1))", visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    { // not query
        ConditionParser parser;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (NumberQuery$1:[NumberTerm: [attr2,[1, 1||1|100|]])",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    { // not expr
        ConditionParser parser;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not ((attr2!=1))", visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    { // not expr
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (((attr1!=1) OR TermQuery$1:[Term:[index_2||aa|100|]]))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    { // not query and not expr
        ConditionParser parser;
        string condStr1 = "{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (TermQuery$1:[Term:[index_2||aa|100|]] AND (attr1!=1))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ(1, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        // not of case expr
        ConditionParser parser;
        string condStr
            = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitLeafCondition) {
    {
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_EQ(0, visitor->getQueryExprs().size());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }

    {
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\",\"params\":[\"$name\",true],\"type\":\"OTHER\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }

    { // toquery error
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"QUERY\", \"params1\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // to expression error
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"attr\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    {
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    { // leaf condition for case op: bool, current supported
        ConditionParser parser;
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitLeafCondition((LeafCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        const AttributeExpression *expr = visitor->getAttributeExpression();
        ASSERT_TRUE(expr != NULL);
        ASSERT_TRUE(expr->getType() == vt_bool);
        using ExprType = suez::turing::VariableTypeTraits<vt_bool, false>::AttrExprType;
        const CaseExpression<ExprType> *caseExpr
            = dynamic_cast<const CaseExpression<ExprType> *>(expr);
        ASSERT_TRUE(caseExpr != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    { // leaf condition for case op: not bool, current not supported
        ConditionParser parser;
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitLeafCondition((LeafCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        const AttributeExpression *expr = visitor->getAttributeExpression();
        ASSERT_TRUE(expr == NULL);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testToTermQuery) {
    { // index not exist
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$index_no_exist\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // number query, string index
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { // number query
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("NumberQuery", query->getQueryName());
    }

    { // term query
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"LIKE\", \"params\":[\"$name\", \"name\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { // condition is not object
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "[]";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // condition op name error
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op1\":\"=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // condition param name error
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params11\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // operator value error
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // params size not equal 2
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$name\", 1, 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // params no index name
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // params both index name
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$name\", \"$name\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { // has cast udf
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string castStr
            = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$name\"]}";
        string condStr = "{\"op\":\"=\", \"params\":[" + castStr + ", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { // has cast udf
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":["
                         "\"$attr2\"]}";
        string condStr = "{\"op\":\"=\", \"params\":[" + castStr + ", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("NumberQuery", query->getQueryName());
    }

    { // has cast udf
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string castStr
            = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$name\"]}";
        string condStr = "{\"op\":\"=\", \"params\":[1," + castStr + "]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { // param has outher udf
        auto visitor = createVisitor();
        autil::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST1\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":["
                         "\"$name\"]}";
        string condStr = "{\"op\":\"=\", \"params\":[1," + castStr + "]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testConditionWithSubAttr) {
    // check isSubExpression flag

    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; // query
        string jsonStr
            = "{\"op\":\"AND\", \"params\":[" + subCondition + ", " + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitAndCondition((AndCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; // query
        string jsonStr = "{\"op\":\"OR\", \"params\":[" + subCondition + ", " + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitOrCondition((OrCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; // query
        string jsonStr = "{\"op\":\"NOT\", \"params\":[" + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitNotCondition((NotCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitUdfCondition_NotUdfToQuery) {
    auto visitor = createVisitor();
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"NOT_EXIST\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(visitor->visitUdfCondition(simpleDoc));
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitUdfCondition_ToQueryFailed) {
    navi::NaviLoggerProvider provider("WARN");
    auto visitor = createVisitor();
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(visitor->visitUdfCondition(simpleDoc));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1,
                      "convert udf [QUERY] to query failed "
                      "[{\"op\":\"QUERY\",\"params\":[],\"type\":\"UDF\"}]. table: invertedTable",
                      traces);
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitUdfCondition_ToQueryFailedAdapter) {
    navi::NaviLoggerProvider provider("WARN");
    auto visitor = createVisitor();
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"contain\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(visitor->visitUdfCondition(simpleDoc));
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitUdfCondition_ToQuerySuccess) {
    navi::NaviLoggerProvider provider("WARN");
    auto visitor = createVisitor();
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"index_2\", \"aa bb stop\", "
                     "\"remove_stopwords:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(visitor->visitUdfCondition(simpleDoc));
}

} // namespace sql
