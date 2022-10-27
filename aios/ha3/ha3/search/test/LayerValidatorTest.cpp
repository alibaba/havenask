#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerValidator.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/queryparser/RequestParser.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class LayerValidatorTest : public TESTBASE {
public:
    LayerValidatorTest();
    ~LayerValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalTest(const std::string& layerStr, 
                      const std::string& hintStr,
                      const std::string& partitionMetaStr,
                      int32_t dimenCount,
                      QuotaMode expectedQuotaMode);

protected:
    LayerValidator _validator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, LayerValidatorTest);


LayerValidatorTest::LayerValidatorTest() { 
}

LayerValidatorTest::~LayerValidatorTest() { 
}

void LayerValidatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void LayerValidatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(LayerValidatorTest, testRankHint) { 
    HA3_LOG(DEBUG, "Begin Test!");
    std::string hintString = "+seller_id";
    RankHint hint;
    ASSERT_TRUE(RequestParser::parseRankHint(hintString, hint));
    ASSERT_TRUE(hint.sortField == "seller_id");
    ASSERT_TRUE(hint.sortPattern == sp_asc);
    RankHint emptyHint;
    ASSERT_TRUE(emptyHint.sortField.empty());
}


TEST_F(LayerValidatorTest, testValidate) { 
    LayerValidator validator;
    
    HA3_LOG(INFO, "testValidate0");
    internalTest(
            "range:cat{1,[,44]}*ends{[555,]}",
            "+ends",
            "+cat;+ends",
            2,
            QM_PER_DOC);
    HA3_LOG(INFO, "testValidate1");
    internalTest(
            "range:cat{1,[,44]}*ends{[33,44], [555,]}",
            "+ends",
            "+cat;+ends",
            2,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate2");
    internalTest(
            "range:cat{1,[,44]}*ends{[33,44], [555,]}",
            "+cat",
            "+cat;+ends",
            2,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate3");
    internalTest(
            "range:cat{1,[,44]}*ends{33,44, 555}",
            "+ends",
            "+cat;+ends",
            2,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate4");
    internalTest(
            "range:cat{1,[,44]}*ends{33}",
            "+ends",
            "+cat;+ends",
            2,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate5");
    internalTest(
            "quota:10",
            "+cat",
            "+cat;+ends",
            0,
            QM_PER_DOC);
    HA3_LOG(INFO, "testValidate6");
    internalTest(
            "range:cat{1,[,44]}*ends{[33,44], 555}",
            "+ends",
            "+cat;+ends",
            0,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate7");
    internalTest(
            "range:cat{1,[,44]}",
            "+ends",
            "+cat;+ends",
            1,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate8");
    internalTest(
            "range:cat{1,44}",
            "+ends",
            "+cat;+ends",
            1,
            QM_PER_DOC);
    HA3_LOG(INFO, "testValidate9");
    internalTest(
            "range:cat{[1,44]}",
            "+ends",
            "+cat;+ends",
            1,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate10");
    internalTest(
            "range:cat{[1,44]}*ends{1,2}*exceed{1}",
            "+ends",
            "+cat;+ends",
            3,
            QM_PER_LAYER);
    HA3_LOG(INFO, "testValidate11");
    internalTest(
            "range:cat{[1,44]}*ends{1,2}*exceed{[1,3]}",
            "+ends",
            "+cat;+ends",
            3,
            QM_PER_LAYER);
}

void LayerValidatorTest::internalTest(const string& layerStr, 
                                      const string& hintStr,
                                      const std::string& partitionMetaStr, 
                                      int32_t dimenCount,
                                      QuotaMode expectedQuotaMode) 
{

    IE_NAMESPACE(index_base)::PartitionMetaPtr meta(new IE_NAMESPACE(index_base)::PartitionMeta);
    StringTokenizer tokens(partitionMetaStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < tokens.getNumTokens(); ++i) {
        if (tokens[i][0] == '+') {
            meta->AddSortDescription((tokens[i].c_str())+1, sp_asc);
        } else if (tokens[i][0] == '-') {
            meta->AddSortDescription((tokens[i].c_str())+1, sp_desc);
        }
    }

    RankHint hint;
    RankHint *hintPtr = NULL;
    if (RequestParser::parseRankHint(hintStr, hint)) {
        hintPtr = &hint;
    }

    ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseLayerClause(layerStr.c_str()));
    QueryLayerClause *layerClause = ctx.stealLayerClause();
    LayerDescription *layerDesc = layerClause->getLayerDescription(0);
    int32_t count = layerDesc->getLayerRangeCount();
    LayerKeyRange *keyRange = layerDesc->getLayerRange(count - 1);
    bool validate = _validator.validateRankHint(meta, hintPtr, keyRange, count);
    QuotaMode quotaMode = validate ? QM_PER_DOC : QM_PER_LAYER;
    ASSERT_EQ(expectedQuotaMode, quotaMode);
    DELETE_AND_SET_NULL(layerClause);
}


END_HA3_NAMESPACE(layer);

