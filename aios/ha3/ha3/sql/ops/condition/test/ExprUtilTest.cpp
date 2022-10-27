#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace suez::turing;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class ExprUtilTest : public TESTBASE {
public:
    ExprUtilTest();
    ~ExprUtilTest();
public:
    void setUp();
    void tearDown();
public:
};

ExprUtilTest::ExprUtilTest() {
}

ExprUtilTest::~ExprUtilTest() {
}

void ExprUtilTest::setUp() {
}

void ExprUtilTest::tearDown() {
}

TEST_F(ExprUtilTest, testTransSqlType) {
    ASSERT_EQ(vt_bool, ExprUtil::transSqlTypeToVariableType("BOOLEAN"));
    ASSERT_EQ(vt_int8, ExprUtil::transSqlTypeToVariableType("TINYINT"));
    ASSERT_EQ(vt_int16, ExprUtil::transSqlTypeToVariableType("SMALLINT"));
    ASSERT_EQ(vt_int32, ExprUtil::transSqlTypeToVariableType("INTEGER"));
    ASSERT_EQ(vt_int64, ExprUtil::transSqlTypeToVariableType("BIGINT"));
    ASSERT_EQ(vt_float, ExprUtil::transSqlTypeToVariableType("DECIMAL"));
    ASSERT_EQ(vt_float, ExprUtil::transSqlTypeToVariableType("FLOAT"));
    ASSERT_EQ(vt_float, ExprUtil::transSqlTypeToVariableType("REAL"));
    ASSERT_EQ(vt_double, ExprUtil::transSqlTypeToVariableType("DOUBLE"));
    ASSERT_EQ(vt_string, ExprUtil::transSqlTypeToVariableType("VARCHAR"));
    ASSERT_EQ(vt_string, ExprUtil::transSqlTypeToVariableType("CHAR"));
    ASSERT_EQ(vt_unknown, ExprUtil::transSqlTypeToVariableType("char"));
}

TEST_F(ExprUtilTest, testParseOutputExprs) {
    autil::mem_pool::Pool pool;
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
    ASSERT_EQ("(tags__host__+10)", exprsMap["tags['host']"].exprStr);
    ASSERT_FALSE(exprsMap.end() == exprsMap.find("_case_"));
    ASSERT_EQ(0, exprsMap["_case_"].exprStr.length());
    ASSERT_EQ(exprsMap["_case_"].exprJson, "{\"type\":\"OTHER\",\"params\":[{\"type\":\"OTHER\",\"params\":[\"$b\",\"-1E+9\"],\"op\":\"<\"},{\"cast_type\":\"FLOAT\",\"type\":\"UDF\",\"params\":[\"0\"],\"op\":\"CAST\"},\"$b\"],\"op\":\"CASE\"}");
}

TEST_F(ExprUtilTest, testIsUdf) {
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(ExprUtil::isUdf(simpleDoc));
    }
    { //operator name error
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op1\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    {// param name error
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params1\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    {  // udf name error
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"NOTUDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
    { // type name error
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type1\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(ExprUtil::isUdf(simpleDoc));
    }
}

TEST_F(ExprUtilTest, testConvertString) {
    string testStr = "()+-*/,|^&@><=.";
    string expectStr = "_______________";
    ExprUtil::convertColumnName(testStr);
    ASSERT_EQ(expectStr, testStr);
}

TEST_F(ExprUtilTest, testIsCastUdf) {
    autil_rapidjson::SimpleDocument simpleDoc;
    string condStr ="{\"op\":\"CAST\", \"params\":[\"name\"], \"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(ExprUtil::isCastUdf(simpleDoc));
}

TEST_F(ExprUtilTest, testIsMultiCastUdf) {
    autil_rapidjson::SimpleDocument simpleDoc;
    string condStr ="{\"op\":\"MULTICAST\", \"params\":[\"name\"], \"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(ExprUtil::isMultiCastUdf(simpleDoc));
}

TEST_F(ExprUtilTest, testIsCaseOp) {
    autil_rapidjson::SimpleDocument simpleDoc;
    string condStr = R"({"op":"CASE","params":[{"op":"=","params":["$warehouse_id",{"cast_type":"BIGINT","op":"CAST","params":[48],"type":"UDF"}],"type":"OTHER"},"$warehouse_id",{"op":"=","params":["$warehouse_id",{"cast_type":"BIGINT","op":"CAST","params":[24],"type":"UDF"}],"type":"OTHER"},"$id",{"cast_type":"BIGINT","op":"CAST","params":[{"cast_type":"BIGINT","op":"CAST","params":[0],"type":"UDF"}],"type":"UDF"}],"type":"OTHER"})";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    ASSERT_TRUE(ExprUtil::isCaseOp(simpleDoc));
}


TEST_F(ExprUtilTest, testToExprStringIn) {
    { // in
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"IN\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"IN\", \"params\":[\"$a\", 1, 2]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1|2\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"IN\", \"params\":[\"$a\", \"1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // in
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"IN\", \"params\":[\"$a\", \"1\", \"2\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("in(a,\"1|2\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // not in
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"NOT IN\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("notin(a,\"1\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }

}

TEST_F(ExprUtilTest, testToExprString) {
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$a\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=1)", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$sum(a)\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("(sum_a_=1)", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(!renameMap.empty());
        ASSERT_EQ("sum(a)", renameMap["sum_a_"]);
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$a\", 1.1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=1.100000)", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$a\", \"1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("(a=\"1\")", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"myFunc\", \"params\":[], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc()", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"myFunc\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc(\"aa\",1)", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // has cast udf
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"CAST\", \"params\":[\"aa\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("\"aa\"", ExprUtil::toExprString(simpleDoc, hasError,
                        errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr1 ="{\"op\":\"myFunc1\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"myFunc2\", \"params\":[" + condStr1 +",\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr2.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("myFunc2(myFunc1(\"aa\",1),\"aa\",1)", ExprUtil::toExprString(simpleDoc,
                        hasError, errorInfo, renameMap));
        ASSERT_TRUE(!hasError);
        ASSERT_TRUE(renameMap.empty());
    }
    { // error op
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op1\":\"myFunc\", \"params\":[\"aa\", 1], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ASSERT_EQ("", ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap));
        ASSERT_TRUE(hasError);
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"myFunc\", \"params\":{}, \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        bool hasError = false;
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        ExprUtil::toExprString(simpleDoc, hasError, errorInfo, renameMap);
        ASSERT_TRUE(hasError);
        ASSERT_TRUE(!errorInfo.empty());
    }
}

END_HA3_NAMESPACE(sql);
