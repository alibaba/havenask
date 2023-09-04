#include "sql/ops/scan/PrimaryKeyScanConditionVisitor.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "indexlib/index/common/Types.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "unittest/unittest.h"

using namespace suez::turing;
using namespace std;

namespace sql {

class PrimaryKeyScanConditionVisitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void getFieldAndIndexName(std::string &fieldName, std::string &indexName);

private:
    suez::turing::TableInfoPtr _tableInfo;
};

void PrimaryKeyScanConditionVisitorTest::setUp() {
    suez::turing::IndexInfos *indexInfos = new suez::turing::IndexInfos();
    _tableInfo.reset(new TableInfo("test"));
    suez::turing::IndexInfo *indexInfo = new suez::turing::IndexInfo();
    indexInfo->indexType = it_primarykey64;
    indexInfo->setIndexName("attr1_idx");
    indexInfo->fieldName = "attr1";
    indexInfos->addIndexInfo(indexInfo);
    _tableInfo->setIndexInfos(indexInfos);
}

void PrimaryKeyScanConditionVisitorTest::tearDown() {}

void PrimaryKeyScanConditionVisitorTest::getFieldAndIndexName(std::string &fieldName,
                                                              std::string &indexName) {
    assert(_tableInfo != NULL);
    auto indexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    assert(indexInfo != NULL);
    fieldName = indexInfo->fieldName;
    indexName = indexInfo->indexName;
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testVisitAndCondition) {
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 2]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string condStr4 = "{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr3 + "," + condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
    }
    // for duplidated pk
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr1 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitAndCondition((AndCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(1, visitor._rawKeyVec.size());
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testVisitOrCondition) {
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 2]}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
    }
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        string condStr4 = "{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr3 + "," + condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
    // for duplidated pk
    {
        ConditionParser parser;
        string condStr1 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 + "," + condStr1 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitOrCondition((OrCondition *)(cond.get()));
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(1, visitor._rawKeyVec.size());
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testVisitNotCondition) {
    { // not query
        ConditionParser parser;
        string condStr = "{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
    { // not expr
        ConditionParser parser;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_FALSE(visitor.stealHasQuery());
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testVisitLeafCondition) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_FALSE(visitor.stealHasQuery());
        ASSERT_TRUE(visitor.needFilter());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_FALSE(visitor.stealHasQuery());
        ASSERT_TRUE(visitor.needFilter());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr1\", 100]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$attr1\", 100, 200]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"contain\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    { // contain with sep
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"contain\", \"params\":[\"$attr1\", \"100#200\", \"#\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    { // ha_in with sep
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"100#200\", \"#\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_FALSE(visitor.needFilter());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testParseIn) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"IN\", \"params\":[\"$attr1\", 100, 200]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"IN","params":["$attr1",{"cast_type":"BIGINT","op":"CAST","params":[100],"type":"UDF"}, 200],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"IN","params":["$attr1", {"cast_type":"BIGINT","op":"CAST","params":["100"],"type":"UDF"},{"cast_type":"BIGINT","op":"CAST","params":[200],"type":"UDF"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testParseUdf) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"contain\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"contain\", \"params\":[\"attr1_idx\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"contain\", \"params\":[\"xxxxxx\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_FALSE(visitor.stealHasQuery());
        ASSERT_EQ(0, visitor.getRawKeyVec().size());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"attr1_idx\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_TRUE(visitor.stealHasQuery());
        ASSERT_EQ(2, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
        ASSERT_EQ("200", visitor.getRawKeyVec()[1]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"xxxxxx\", \"100|200\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        visitor.visitLeafCondition(&cond);
        ASSERT_FALSE(visitor.stealHasQuery());
        ASSERT_EQ(0, visitor.getRawKeyVec().size());
    }
}

TEST_F(PrimaryKeyScanConditionVisitorTest, testParsePk) {
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_FALSE(visitor.parseKey(simpleDoc));
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[\"$attr1\", 100]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr = "{\"op\":\"=\", \"params\":[100, \"$attr1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("100", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"=","params":[{"cast_type":"BIGINT","op":"CAST","params":["$attr1"],"type":"UDF"},601761753904],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("601761753904", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"=","params":["$attr1",{"cast_type":"BIGINT","op":"CAST","params":[601761753904],"type":"UDF"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("601761753904", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"=","params":[{"cast_type":"BIGINT","op":"CAST","params":["$attr1"],"type":"UDF"},{"cast_type":"BIGINT","op":"CAST","params":[601761753904],"type":"UDF"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("601761753904", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"IN","params":["$attr1",{"cast_type":"BIGINT","op":"CAST","params":[1],"type":"UDF"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_TRUE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(1, visitor.getRawKeyVec().size());
        ASSERT_EQ("1", visitor.getRawKeyVec()[0]);
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"IN","params":["notkey",{"cast_type":"BIGINT","op":"CAST","params":[1],"type":"UDF"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_FALSE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(0, visitor.getRawKeyVec().size());
    }
    {
        autil::SimpleDocument simpleDoc;
        string condStr
            = R"json({"op":"IN","params":["$nid", {"hello": "world"}],"type":"OTHER"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        std::string fieldName;
        std::string indexName;
        getFieldAndIndexName(fieldName, indexName);
        PrimaryKeyScanConditionVisitor visitor(fieldName, indexName);
        ASSERT_FALSE(visitor.parseKey(simpleDoc));
        ASSERT_EQ(0, visitor.getRawKeyVec().size());
    }
}

} // namespace sql
