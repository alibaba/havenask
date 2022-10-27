#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/common/VirtualAttributeClause.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);

class VirtualAttributeBisonParserTest : public TESTBASE {
public:
    VirtualAttributeBisonParserTest();
    ~VirtualAttributeBisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, VirtualAttributeBisonParserTest);


VirtualAttributeBisonParserTest::VirtualAttributeBisonParserTest() { 
}

VirtualAttributeBisonParserTest::~VirtualAttributeBisonParserTest() { 
}

void VirtualAttributeBisonParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void VirtualAttributeBisonParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(VirtualAttributeBisonParserTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ClauseParserContext ctx;
    bool ret = ctx.parseVirtualAttributeClause("va1:fun(\"#\", a);va2:a2 + 1");
    ASSERT_TRUE(ret);
    VirtualAttributeClause *virtualAttributeClause = 
        ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> 
        virtualAttributeClausePtr1(virtualAttributeClause);
    ASSERT_TRUE(virtualAttributeClause);
    ASSERT_EQ(size_t(2), 
                         virtualAttributeClause->getVirtualAttributeSize());
    const vector<VirtualAttribute*>& virtualAttributees = 
        virtualAttributeClause->getVirtualAttributes();
    ASSERT_EQ(string("va1"), 
                         virtualAttributees[0]->getVirtualAttributeName());
    ASSERT_EQ(string("va2"), 
                         virtualAttributees[1]->getVirtualAttributeName());

    FuncSyntaxExpr *funcExpr = dynamic_cast<FuncSyntaxExpr*>(
            virtualAttributees[0]->getVirtualAttributeSyntaxExpr());
    ASSERT_TRUE(funcExpr);
    ASSERT_EQ(string("fun"), funcExpr->getFuncName());
    ASSERT_EQ(string("fun(\"#\" , a)"), funcExpr->getExprString());
    const FuncSyntaxExpr::SubExprType& subExprType = funcExpr->getParam();
    ASSERT_EQ((size_t)2, subExprType.size());
    const AtomicSyntaxExpr *atomicSyntaxExpr = 
        dynamic_cast<AtomicSyntaxExpr*>(subExprType[0]);
    ASSERT_EQ(SYNTAX_EXPR_TYPE_FUNC_ARGUMENT, 
                         atomicSyntaxExpr->getSyntaxExprType());
    atomicSyntaxExpr = dynamic_cast<AtomicSyntaxExpr*>(subExprType[1]);
    ASSERT_EQ(SYNTAX_EXPR_TYPE_ATOMIC_ATTR,
                         atomicSyntaxExpr->getSyntaxExprType());

    BinarySyntaxExpr* binarySyntaxExpr = 
        dynamic_cast<BinarySyntaxExpr*>(
                virtualAttributees[1]->getVirtualAttributeSyntaxExpr());
    ASSERT_TRUE(binarySyntaxExpr);
    ASSERT_EQ(SYNTAX_EXPR_TYPE_ADD,
                         binarySyntaxExpr->getSyntaxExprType());
    ASSERT_EQ(string("(a2+1)"), 
                         binarySyntaxExpr->getExprString());

    atomicSyntaxExpr = 
        dynamic_cast<const AtomicSyntaxExpr*>(binarySyntaxExpr->getLeftExpr());
    ASSERT_EQ(SYNTAX_EXPR_TYPE_ATOMIC_ATTR, 
                         atomicSyntaxExpr->getSyntaxExprType());

    atomicSyntaxExpr = 
        dynamic_cast<const AtomicSyntaxExpr*>(binarySyntaxExpr->getRightExpr());
    ASSERT_EQ(SYNTAX_EXPR_TYPE_CONST_VALUE, 
                         atomicSyntaxExpr->getSyntaxExprType());
}

TEST_F(VirtualAttributeBisonParserTest, testParseSuccess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ClauseParserContext ctx;

    bool ret = ctx.parseVirtualAttributeClause("va1:a1;");
    ASSERT_TRUE(ret);
    VirtualAttributeClause *virtualAttributeClause = 
        ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> virtualAttributeClausePtr1(
            virtualAttributeClause);
    ASSERT_TRUE(virtualAttributeClause);
    ASSERT_EQ(size_t(1), 
                         virtualAttributeClause->getVirtualAttributeSize());
    const vector<VirtualAttribute*>& virtualAttributees = 
        virtualAttributeClause->getVirtualAttributes();
    ASSERT_EQ(string("va1"), 
                         virtualAttributees[0]->getVirtualAttributeName());
    ASSERT_EQ(string("a1"), 
                         virtualAttributees[0]->getVirtualAttributeSyntaxExpr()
                         ->getExprString());

    ret = ctx.parseVirtualAttributeClause(";va1:a1");
    ASSERT_TRUE(ret);
    virtualAttributeClause = ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> virtualAttributeClausePtr2(
            virtualAttributeClause);
    ASSERT_TRUE(virtualAttributeClause);
    ASSERT_EQ(size_t(1), 
                         virtualAttributeClause->getVirtualAttributeSize());
    const vector<VirtualAttribute*>& virtualAttributees2 = 
        virtualAttributeClause->getVirtualAttributes();
    ASSERT_EQ(string("va1"), 
                         virtualAttributees2[0]->getVirtualAttributeName());
    ASSERT_EQ(string("a1"), 
                         virtualAttributees2[0]->
                         getVirtualAttributeSyntaxExpr()->getExprString());

    ret = ctx.parseVirtualAttributeClause(";va1:a1;va2:a2");
    ASSERT_TRUE(ret);
    virtualAttributeClause = ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> virtualAttributeClausePtr3(
            virtualAttributeClause);
    ASSERT_EQ(size_t(2), 
                         virtualAttributeClause->getVirtualAttributeSize());
    const vector<VirtualAttribute*>& virtualAttributees3 = 
        virtualAttributeClause->getVirtualAttributes();
    ASSERT_EQ(string("va1"), 
                         virtualAttributees3[0]->getVirtualAttributeName());
    ASSERT_EQ(string("va2"), 
                         virtualAttributees3[1]->getVirtualAttributeName());

    ret = ctx.parseVirtualAttributeClause("va1:a1;va2:a2;");
    ASSERT_TRUE(ret);
    virtualAttributeClause = ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> 
        virtualAttributeClausePtr4(virtualAttributeClause);
    ASSERT_EQ(size_t(2), 
                         virtualAttributeClause->getVirtualAttributeSize());

    ret = ctx.parseVirtualAttributeClause("va1:a1;va2:va1 + 1;va3:a3;va4:fun(va1);");
    ASSERT_TRUE(ret);
    virtualAttributeClause = ctx.stealVirtualAttributeClause();
    unique_ptr<VirtualAttributeClause> 
        virtualAttributeClausePtr5(virtualAttributeClause);
    ASSERT_EQ(size_t(4), 
                         virtualAttributeClause->getVirtualAttributeSize());

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("va1:id;va2:sum(va1,company_id);va3:sum(company_id)");
        ASSERT_TRUE(ret);
    }
}

TEST_F(VirtualAttributeBisonParserTest, testParseFailed) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause(";");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected end of file") 
                       != string::npos);
    }

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause(" ");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected end of file") 
                       != string::npos);
    }
    
    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("va1:a1;va1:a2");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("same name virtualAttribute may has "
                        "already existed") 
                       != string::npos);
    }

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("va1:;va2:a2");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected ';'") 
                       != string::npos);
    }

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("va1:a1;:a2");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected ':'")
                        != string::npos);
    }

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("va1:func(#)");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected '#'")
                        != string::npos);
    }

    {
        ClauseParserContext ctx;
        bool ret = ctx.parseVirtualAttributeClause("v#1:func(a)");
        ASSERT_TRUE(!ret);
        const ErrorResult &er = ctx.getErrorResult();
        ASSERT_EQ(ERROR_VIRTUALATTRIBUTE_CLAUSE, er.getErrorCode());
        ASSERT_TRUE(er.getErrorMsg().find("unexpected '#'")
                        != string::npos);
    }
}
END_HA3_NAMESPACE(queryparser);

