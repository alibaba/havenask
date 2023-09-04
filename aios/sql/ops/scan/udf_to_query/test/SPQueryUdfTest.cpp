#include "sql/ops/scan/udf_to_query/SpQueryUdf.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/common/Query.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/queryparser/QueryParser.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "unittest/unittest.h"

using namespace std;
using namespace isearch::queryparser;

namespace sql {

class SPQueryUdfTest : public TESTBASE {
public:
    SPQueryUdfTest();
    ~SPQueryUdfTest();

public:
    void setUp();
    void tearDown();

protected:
    void testParse(const char *query, const string &expect);

protected:
    unique_ptr<SPQueryUdf> _spQueryUdfPtr;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SPQueryUdfTest);

SPQueryUdfTest::SPQueryUdfTest() {}

SPQueryUdfTest::~SPQueryUdfTest() {}

void SPQueryUdfTest::setUp() {
    _spQueryUdfPtr.reset(new SPQueryUdf("default"));
}

void SPQueryUdfTest::tearDown() {}

void SPQueryUdfTest::testParse(const char *query, const string &expect) {
    unique_ptr<ParserContext> ctx(_spQueryUdfPtr->parse(query));
    ASSERT_TRUE(nullptr != ctx);
    const auto &querys = ctx->getQuerys();
    ASSERT_EQ(1u, querys.size());
    ASSERT_TRUE(nullptr != querys[0]);
    const auto &actual = querys[0]->toString();
    ASSERT_EQ(expect, actual);
}

TEST_F(SPQueryUdfTest, testAttachExpr) {
    EXPECT_NO_FATAL_FAILURE(
        testParse("+ju_sales_site:1", "TermQuery$1:[Term:[ju_sales_site||1|100|]]"));
    EXPECT_NO_FATAL_FAILURE(
        testParse("ju_sales_site:1+", "TermQuery$1:[Term:[ju_sales_site||1|100|]]"));
}

TEST_F(SPQueryUdfTest, testAndExpr) {
    EXPECT_NO_FATAL_FAILURE(testParse("ju_sales_site:1+ju_item_type:0",
                                      "AndQuery:[TermQuery$1:[Term:[ju_sales_site||1|100|]], "
                                      "TermQuery$1:[Term:[ju_item_type||0|100|]], ]"));
}

TEST_F(SPQueryUdfTest, testOrExpr) {
    EXPECT_NO_FATAL_FAILURE(testParse("ju_sales_site:1|ju_sales_site:2",
                                      "OrQuery:[TermQuery$1:[Term:[ju_sales_site||1|100|]], "
                                      "TermQuery$1:[Term:[ju_sales_site||2|100|]], ]"));

    EXPECT_NO_FATAL_FAILURE(testParse("ju_sales_site:1|2|3",
                                      "OrQuery:[OrQuery:[TermQuery$1:[Term:[ju_sales_site||1|100|]]"
                                      ", TermQuery$1:[Term:[ju_sales_site||2|100|]], ], "
                                      "TermQuery$1:[Term:[ju_sales_site||3|100|]], ]"));
}

TEST_F(SPQueryUdfTest, testAndNotExpr) {
    EXPECT_NO_FATAL_FAILURE(testParse("ju_sales_site:1+!ju_item_labels:sys_cst",
                                      "AndNotQuery:[TermQuery$1:[Term:[ju_sales_site||1|100|]], "
                                      "TermQuery$1:[Term:[ju_item_labels||sys_cst|100|]], ]"));
}

TEST_F(SPQueryUdfTest, testParentheses) {
    EXPECT_NO_FATAL_FAILURE(
        testParse("(ju_sales_site:1)", "TermQuery$1:[Term:[ju_sales_site||1|100|]]"));
    EXPECT_NO_FATAL_FAILURE(
        testParse("(ju_sales_site:1|ju_sales_site:1)+status:0",
                  "AndQuery:[OrQuery:[TermQuery$1:[Term:[ju_sales_site||1|100|]],"
                  " TermQuery$1:[Term:[ju_sales_site||1|100|]], "
                  "], TermQuery$1:[Term:[status||0|100|]], ]"));
}

TEST_F(SPQueryUdfTest, testSpQuery) {
    EXPECT_NO_FATAL_FAILURE(testParse("(+(ju_sales_site:1)+(status:0)+(ju_labels:sys_bgroup_single)"
                                      "+!ju_labels:jhsqckabrand2019+!ju_labels:JHS_"
                                      "TFSELF_TAG+(ju_block_type:1))",
                                      "AndQuery:[AndNotQuery:[AndNotQuery:[AndQuery:[AndQuery:["
                                      "TermQuery$1:[Term:[ju_sales_site||1|100|]], "
                                      "TermQuery$1:[Term:[status||0|100|]], ], "
                                      "TermQuery$1:[Term:[ju_labels||sys_bgroup_single|100|]], ], "
                                      "TermQuery$1:[Term:[ju_labels||jhsqckabrand2019|100|]], ], "
                                      "TermQuery$1:[Term:[ju_labels||JHS_TFSELF_TAG|100|]], ], "
                                      "TermQuery$1:[Term:[ju_block_type||1|100|]], ]"));
    EXPECT_NO_FATAL_FAILURE(testParse(
        "(+(ju_sales_site:1|ju_sales_site:2|ju_sales_site:37)+(ju_item_type:0)+!ju_item_labels:sys_"
        "cst+!ju_item_"
        "labels:jhs_tfself_tag+(ju_item_labels:jhs_feeds|ju_item_labels:jhs_feeds_tuan_items))",
        "AndQuery:[AndNotQuery:[AndNotQuery:[AndQuery:[OrQuery:[OrQuery:[TermQuery$1:[Term:[ju_"
        "sales_"
        "site||1|100|]]"
        ", TermQuery$1:[Term:[ju_sales_site||2|100|]], ], "
        "TermQuery$1:[Term:[ju_sales_site||37|100|]], "
        "], "
        "TermQuery$1:[Term:[ju_item_type||0|100|]], ], "
        "TermQuery$1:[Term:[ju_item_labels||sys_cst|100|]], ], "
        "TermQuery$1:[Term:[ju_item_labels||jhs_tfself_tag|100|]], ], "
        "OrQuery:[TermQuery$1:[Term:[ju_item_labels||jhs_feeds|100|]], "
        "TermQuery$1:[Term:[ju_item_labels||jhs_feeds_tuan_items|100|]], ], ]"));
    EXPECT_NO_FATAL_FAILURE(testParse(
        "(+(ju_sales_site:1#ju_sales_site:2#ju_sales_site:37)+(ju_item_type:0)+!ju_item_labels:sys_"
        "cst+!ju_item_"
        "labels:jhs_tfself_tag+(ju_item_labels:jhs_feeds#ju_item_labels:jhs_feeds_tuan_items))",
        "AndQuery:[AndNotQuery:[AndNotQuery:[AndQuery:[OrQuery:[OrQuery:[TermQuery$1:[Term:[ju_"
        "sales_"
        "site||1|100|]]"
        ", TermQuery$1:[Term:[ju_sales_site||2|100|]], ], "
        "TermQuery$1:[Term:[ju_sales_site||37|100|]], "
        "], "
        "TermQuery$1:[Term:[ju_item_type||0|100|]], ], "
        "TermQuery$1:[Term:[ju_item_labels||sys_cst|100|]], ], "
        "TermQuery$1:[Term:[ju_item_labels||jhs_tfself_tag|100|]], ], "
        "OrQuery:[TermQuery$1:[Term:[ju_item_labels||jhs_feeds|100|]], "
        "TermQuery$1:[Term:[ju_item_labels||jhs_feeds_tuan_items|100|]], ], ]"));

    EXPECT_NO_FATAL_FAILURE(
        testParse("((auction_type:b|auction_type:a)+!(auction_tag:363266))|(region:420115|4201)",
                  "OrQuery:[AndNotQuery:[OrQuery:[TermQuery$1:[Term:[auction_type||b|100|]], "
                  "TermQuery$1:[Term:[auction_type||a|100|]], ], "
                  "TermQuery$1:[Term:[auction_tag||363266|100|]], ], "
                  "OrQuery:[TermQuery$1:[Term:[region||420115|100|]], "
                  "TermQuery$1:[Term:[region||4201|100|]], ], ]"));

    EXPECT_NO_FATAL_FAILURE(testParse("(((region:120105)|(xuanpin_tag:5))+(is_vertical:0))",
                                      "AndQuery:[OrQuery:[TermQuery$1:[Term:[region||120105|100|]],"
                                      " TermQuery$1:[Term:[xuanpin_tag||5|100|]], ], "
                                      "TermQuery$1:[Term:[is_vertical||0|100|]], ]"));

    EXPECT_NO_FATAL_FAILURE(testParse(
        "((auction_type:b|region:a|c|123)+!(auction_tag:363266))|(region:420115|4201)",
        "OrQuery:[AndNotQuery:[OrQuery:[OrQuery:[OrQuery:[TermQuery$1:[Term:[auction_type||b|100|]]"
        ", TermQuery$1:[Term:[region||a|100|]], ], TermQuery$1:[Term:[region||c|100|]], ], "
        "TermQuery$1:[Term:[region||123|100|]], ], TermQuery$1:[Term:[auction_tag||363266|100|]], "
        "], OrQuery:[TermQuery$1:[Term:[region||420115|100|]], "
        "TermQuery$1:[Term:[region||4201|100|]], ], ]"));
}

} // namespace sql
