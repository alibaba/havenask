#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/scan/SpQueryUdf.h>

using namespace std;
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(sql);

class SPQueryUdfTest : public TESTBASE {
public:
    SPQueryUdfTest();
    ~SPQueryUdfTest();
public:
    void setUp();
    void tearDown();
protected:
    void testParse(const char* query, const string& expect);
protected:
    unique_ptr<SPQueryUdf> _spQueryUdfPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SPQueryUdfTest);

SPQueryUdfTest::SPQueryUdfTest() {
}

SPQueryUdfTest::~SPQueryUdfTest() {
}

void SPQueryUdfTest::setUp() {
    _spQueryUdfPtr.reset(new SPQueryUdf("default"));
}

void SPQueryUdfTest::tearDown() {
}

void SPQueryUdfTest::testParse(const char* query, const string& expect) {
    unique_ptr<ParserContext> ctx(_spQueryUdfPtr->parse(query));
    ASSERT_TRUE(nullptr != ctx);
    const auto &querys = ctx->getQuerys();
    ASSERT_EQ(1u, querys.size());
    ASSERT_TRUE(nullptr != querys[0]);
    const auto& actual = querys[0]->toString();
    ASSERT_EQ(expect, actual);
}

TEST_F(SPQueryUdfTest, testAttachExpr) {
    testParse(
        "+ju_sales_site:1",
        "TermQuery:[Term:[ju_sales_site||1|100|]]"
    );
    testParse(
        "ju_sales_site:1+",
        "TermQuery:[Term:[ju_sales_site||1|100|]]"
    );
}

TEST_F(SPQueryUdfTest, testAndExpr) {
    testParse(
        "ju_sales_site:1+ju_item_type:0",
        "AndQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_item_type||0|100|]], ]"
    );
}

TEST_F(SPQueryUdfTest, testOrExpr) {
    testParse(
        "ju_sales_site:1|ju_sales_site:2",
        "OrQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_sales_site||2|100|]], ]"
    );

    testParse(
        "ju_sales_site:1|2|3",
        "OrQuery:[OrQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_sales_site||2|100|]], ], TermQuery:[Term:[ju_sales_site||3|100|]], ]"
    );
}

TEST_F(SPQueryUdfTest, testAndNotExpr) {
    testParse(
        "ju_sales_site:1+!ju_item_labels:sys_cst",
        "AndNotQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_item_labels||sys_cst|100|]], ]"
    );
}

TEST_F(SPQueryUdfTest, testParentheses) {
    testParse(
        "(ju_sales_site:1)",
        "TermQuery:[Term:[ju_sales_site||1|100|]]"
    );
    testParse(
        "(ju_sales_site:1|ju_sales_site:1)+status:0",
        "AndQuery:[OrQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_sales_site||1|100|]], ], TermQuery:[Term:[status||0|100|]], ]"
    );
}

TEST_F(SPQueryUdfTest, testSpQuery) {
    testParse(
        "(+(ju_sales_site:1)+(status:0)+(ju_labels:sys_bgroup_single)+!ju_labels:jhsqckabrand2019+!ju_labels:JHS_TFSELF_TAG+(ju_block_type:1))",
        "AndQuery:[AndNotQuery:[AndNotQuery:[AndQuery:[AndQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[status||0|100|]], ], TermQuery:[Term:[ju_labels||sys_bgroup_single|100|]], ], TermQuery:[Term:[ju_labels||jhsqckabrand2019|100|]], ], TermQuery:[Term:[ju_labels||JHS_TFSELF_TAG|100|]], ], TermQuery:[Term:[ju_block_type||1|100|]], ]"
    );
    testParse(
        "(+(ju_sales_site:1|ju_sales_site:2|ju_sales_site:37)+(ju_item_type:0)+!ju_item_labels:sys_cst+!ju_item_labels:jhs_tfself_tag+(ju_item_labels:jhs_feeds|ju_item_labels:jhs_feeds_tuan_items))",
        "AndQuery:[AndNotQuery:[AndNotQuery:[AndQuery:[OrQuery:[OrQuery:[TermQuery:[Term:[ju_sales_site||1|100|]], TermQuery:[Term:[ju_sales_site||2|100|]], ], TermQuery:[Term:[ju_sales_site||37|100|]], ], TermQuery:[Term:[ju_item_type||0|100|]], ], TermQuery:[Term:[ju_item_labels||sys_cst|100|]], ], TermQuery:[Term:[ju_item_labels||jhs_tfself_tag|100|]], ], OrQuery:[TermQuery:[Term:[ju_item_labels||jhs_feeds|100|]], TermQuery:[Term:[ju_item_labels||jhs_feeds_tuan_items|100|]], ], ]"
    );

    testParse(
        "((auction_type:b|auction_type:a)+!(auction_tag:363266))|(region:420115|4201)",
        "OrQuery:[AndNotQuery:[OrQuery:[TermQuery:[Term:[auction_type||b|100|]], TermQuery:[Term:[auction_type||a|100|]], ], TermQuery:[Term:[auction_tag||363266|100|]], ], OrQuery:[TermQuery:[Term:[region||420115|100|]], TermQuery:[Term:[region||4201|100|]], ], ]"
    );

    testParse(
        "(((region:120105)|(xuanpin_tag:5))+(is_vertical:0))",
        "AndQuery:[OrQuery:[TermQuery:[Term:[region||120105|100|]], TermQuery:[Term:[xuanpin_tag||5|100|]], ], TermQuery:[Term:[is_vertical||0|100|]], ]"
    );


    testParse(
        "((auction_type:b|region:a|c|123)+!(auction_tag:363266))|(region:420115|4201)",
        "OrQuery:[AndNotQuery:[OrQuery:[OrQuery:[OrQuery:[TermQuery:[Term:[auction_type||b|100|]], TermQuery:[Term:[region||a|100|]], ], TermQuery:[Term:[region||c|100|]], ], TermQuery:[Term:[region||123|100|]], ], TermQuery:[Term:[auction_tag||363266|100|]], ], OrQuery:[TermQuery:[Term:[region||420115|100|]], TermQuery:[Term:[region||4201|100|]], ], ]"
    );
}

END_HA3_NAMESPACE(sql);
