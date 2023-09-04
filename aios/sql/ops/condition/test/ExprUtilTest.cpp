#include "sql/ops/condition/ExprUtil.h"

#include <iosfwd>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "autil/legacy/RapidJsonCommon.h"
#include "navi/util/NaviTestPool.h"
#include "sql/common/common.h"
#include "suez/turing/expression/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;

namespace sql {

class ExprUtilTest : public TESTBASE {
public:
    ExprUtilTest();
    ~ExprUtilTest();

public:
    void setUp();
    void tearDown();

public:
};

ExprUtilTest::ExprUtilTest() {}

ExprUtilTest::~ExprUtilTest() {}

void ExprUtilTest::setUp() {}

void ExprUtilTest::tearDown() {}

TEST_F(ExprUtilTest, testTransSqlType) {
    ASSERT_EQ(pair(vt_bool, false), ExprUtil::transSqlTypeToVariableType("BOOLEAN"));
    ASSERT_EQ(pair(vt_int8, false), ExprUtil::transSqlTypeToVariableType("TINYINT"));
    ASSERT_EQ(pair(vt_int16, false), ExprUtil::transSqlTypeToVariableType("SMALLINT"));
    ASSERT_EQ(pair(vt_int32, false), ExprUtil::transSqlTypeToVariableType("INTEGER"));
    ASSERT_EQ(pair(vt_int64, false), ExprUtil::transSqlTypeToVariableType("BIGINT"));
    ASSERT_EQ(pair(vt_float, false), ExprUtil::transSqlTypeToVariableType("DECIMAL"));
    ASSERT_EQ(pair(vt_float, false), ExprUtil::transSqlTypeToVariableType("FLOAT"));
    ASSERT_EQ(pair(vt_float, false), ExprUtil::transSqlTypeToVariableType("REAL"));
    ASSERT_EQ(pair(vt_double, false), ExprUtil::transSqlTypeToVariableType("DOUBLE"));
    ASSERT_EQ(pair(vt_string, false), ExprUtil::transSqlTypeToVariableType("VARCHAR"));
    ASSERT_EQ(pair(vt_string, false), ExprUtil::transSqlTypeToVariableType("CHAR"));
    ASSERT_EQ(pair(vt_unknown, false), ExprUtil::transSqlTypeToVariableType("char"));

    ASSERT_EQ(pair(vt_bool, true), ExprUtil::transSqlTypeToVariableType("ARRAY(BOOLEAN)"));
    ASSERT_EQ(pair(vt_int8, true), ExprUtil::transSqlTypeToVariableType("ARRAY(TINYINT)"));
    ASSERT_EQ(pair(vt_int16, true), ExprUtil::transSqlTypeToVariableType("ARRAY(SMALLINT)"));
    ASSERT_EQ(pair(vt_int32, true), ExprUtil::transSqlTypeToVariableType("ARRAY(INTEGER)"));
    ASSERT_EQ(pair(vt_int64, true), ExprUtil::transSqlTypeToVariableType("ARRAY(BIGINT)"));
    ASSERT_EQ(pair(vt_float, true), ExprUtil::transSqlTypeToVariableType("ARRAY(DECIMAL)"));
    ASSERT_EQ(pair(vt_float, true), ExprUtil::transSqlTypeToVariableType("ARRAY(FLOAT)"));
    ASSERT_EQ(pair(vt_float, true), ExprUtil::transSqlTypeToVariableType("ARRAY(REAL)"));
    ASSERT_EQ(pair(vt_double, true), ExprUtil::transSqlTypeToVariableType("ARRAY(DOUBLE)"));
    ASSERT_EQ(pair(vt_string, true), ExprUtil::transSqlTypeToVariableType("ARRAY(VARCHAR)"));
    ASSERT_EQ(pair(vt_string, true), ExprUtil::transSqlTypeToVariableType("ARRAY(CHAR)"));
}

TEST_F(ExprUtilTest, testParseOutputExprs) {
    autil::mem_pool::PoolAsan pool;
    std::string outputFieldExprs = R"json({
    "$id1" : {"op" : "+", "params":["$id1", 10]},
    "$a" : "$a",
    "$b" : "$id",
    "$c" : "b",
    "$e" : 123,
    "$f" : 11.1,
    "f(a)": {"op":"f", "params":["$a"], "type":"UDF"},
    "tags['app']": {
        "op": "ITEM",
        "params": [
            "$tags",
            "app"
        ],
        "type": "OTHER"
    },
    "tags['host']" : {
        "op" : "+",
        "params":[
            {
                "op": "ITEM",
                "params": [
                    "$tags",
                    "host"
                ],
                "type": "OTHER"
            },
            10
        ]
    },
    "$_case_": {"type":"OTHER","params":[{"type":"OTHER","params":["$b","-1E+9"],"op":"<"},{"cast_type":"FLOAT","type":"UDF","params":["0"],"op":"CAST"},"$b"],"op":"CASE"}
})json";
    std::map<std::string, ExprEntity> exprsMap;
    ASSERT_TRUE(ExprUtil::parseOutputExprs(&pool, outputFieldExprs, exprsMap));
    ASSERT_EQ("(id1+10)", exprsMap["id1"].exprStr);
    ASSERT_EQ("a", exprsMap["a"].exprStr); // a as a
    ASSERT_EQ("id", exprsMap["b"].exprStr);
    ASSERT_EQ("\"b\"", exprsMap["c"].exprStr);
    ASSERT_EQ("123", exprsMap["e"].exprStr);
    ASSERT_EQ("11.100000", exprsMap["f"].exprStr);
    ASSERT_EQ("f(a)", exprsMap["f(a)"].exprStr);
    ASSERT_EQ(exprsMap.end(), exprsMap.find("tags['app']"));
    std::string token = "tags['host']";
    ExprUtil::convertColumnName(token);
    ASSERT_EQ("(" + token + "+10)", exprsMap["tags['host']"].exprStr);
    ASSERT_FALSE(exprsMap.end() == exprsMap.find("_case_"));
    ASSERT_EQ(0, exprsMap["_case_"].exprStr.length());
    ASSERT_EQ(exprsMap["_case_"].exprJson,
              "{\"type\":\"OTHER\",\"params\":[{\"type\":\"OTHER\",\"params\":[\"$b\",\"-1E+9\"],"
              "\"op\":\"<\"},{\"cast_"
              "type\":\"FLOAT\",\"type\":\"UDF\",\"params\":[\"0\"],\"op\":\"CAST\"},\"$b\"],"
              "\"op\":\"CASE\"}");
}

TEST_F(ExprUtilTest, testIsUdf) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(ExprUtil::isUdf(simpleDoc));
    }
    { // operator name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op1\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    { // param name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"QUERY\", \"params1\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    { // udf name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"NOTUDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    { // type name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type1\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
}

TEST_F(ExprUtilTest, testIsInOp) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"IN\", \"params\":[\"name\", \"aa\", \"bb\"], \"type\":\"OTHER\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(ExprUtil::isInOp(simpleDoc));
    }
    { // operator name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"xx\", \"params\":[\"name\", \"aa\", \"bb\"], \"type\":\"OTHER\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isInOp(simpleDoc));
    }
    { // param name error
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"IN\", \"params1\":[\"name\", \"aa\", \"bb\"], \"type\":\"OTHER\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isInOp(simpleDoc));
    }
}

TEST_F(ExprUtilTest, testConvertString) {
    {
        string testStr = "()+-*/,|^&@><=!";
        string newTestStr = testStr;
        ExprUtil::convertColumnName(newTestStr);
        ASSERT_NE(testStr, newTestStr);
    }
    {
        string testStr = "tags['host!']";
        string newTestStr = testStr;
        ExprUtil::convertColumnName(newTestStr);
        ASSERT_NE(testStr, newTestStr);
    }
}

TEST_F(ExprUtilTest, testIsSameUdf) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"CAST\", \"params\":[\"name\"], \"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(ExprUtil::isSameUdf(simpleDoc, SQL_UDF_CAST_OP));
}

TEST_F(ExprUtilTest, testIsSameUdf_FAILED) {
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"unknown\", \"params\":[\"name\"], \"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_FALSE(ExprUtil::isSameUdf(simpleDoc, SQL_UDF_CAST_OP));
}

TEST_F(ExprUtilTest, testIsCaseOp) {
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"({"op":"CASE","params":[{"op":"=","params":["$warehouse_id",{"cast_type":"BIGINT","op":"CAST","params":[48],"type":"UDF"}],"type":"OTHER"},"$warehouse_id",{"op":"=","params":["$warehouse_id",{"cast_type":"BIGINT","op":"CAST","params":[24],"type":"UDF"}],"type":"OTHER"},"$id",{"cast_type":"BIGINT","op":"CAST","params":[{"cast_type":"BIGINT","op":"CAST","params":[0],"type":"UDF"}],"type":"UDF"}],"type":"OTHER"})";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(ExprUtil::isCaseOp(simpleDoc));
}

TEST_F(ExprUtilTest, testToExprStringIn) {
    { // in
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1\")", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$a\", 1, 2]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1|2\")",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$a\", \"1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1\")", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$a\", \"1\", \"2\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1|2\")",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // not in
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"NOT IN\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("notin(a,\"1\")",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
}

TEST_F(ExprUtilTest, testToExprString) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=1)", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$sum(a)\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        std::string token = "sum(a)";
        ExprUtil::convertColumnName(token);
        ASSERT_EQ("(" + token + "=1)",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(!renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$a\", 1.1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=1.100000)",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$a\", \"1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=\"1\")", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"myFunc\", \"params\":[], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc()", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"myFunc\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc(\"aa\",1)",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // has cast udf
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"CAST\", \"params\":[\"aa\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("\"aa\"", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr1 = "{\"op\":\"myFunc1\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        string condStr2
            = "{\"op\":\"myFunc2\", \"params\":[" + condStr1 + ",\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr2.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc2(myFunc1(\"aa\",1),\"aa\",1)",
                  ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // error op
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op1\":\"myFunc\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ASSERT_EQ("", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(hasError);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"myFunc\", \"params\":{}, \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::unordered_map<std::string, std::string> renameMap;
        ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap);
        ASSERT_TRUE(hasError);
        ASSERT_TRUE(!errorInfo.empty());
    }
}

} // namespace sql
