#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AggregatorCreator.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/function/FunctionManager.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/TupleAggregator.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/func_expression/FunctionProvider.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(queryparser);
IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);

class AggregatorCreatorTest : public TESTBASE {
public:
    AggregatorCreatorTest();
    ~AggregatorCreatorTest();
public:
    void setUp();
    void tearDown();

protected:
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;
    suez::turing::FunctionManagerPtr _funcManagerPtr;
    suez::turing::FunctionInterfaceCreator *_funcCreator;
    func_expression::FunctionProvider *_functionProvider;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AggregatorCreatorTest);


AggregatorCreatorTest::AggregatorCreatorTest() :
    _attributeExpressionCreator(NULL)
{
    _pool = new autil::mem_pool::Pool(1024);
}

AggregatorCreatorTest::~AggregatorCreatorTest() {
    if (_attributeExpressionCreator) {
        delete _attributeExpressionCreator;
    }
    delete _pool;
}

void AggregatorCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

    FakeIndex fakeIndex;
    fakeIndex.attributes = "uid_int8 : int8_t : -8, 2;"
                           "uid_int16 : int16_t : -16, 2;"
                           "uid_int32 : int32_t : -32, 2;"
                           "uid_int64 : int64_t : -64, 2;"
                           "uid_uint8 : uint8_t : 8, 2;"
                           "uid_uint16 : uint16_t : 16, 2;"
                           "uid_uint32 : uint32_t : 32, 2;"
                           "uid_uint64 : uint64_t : 64, 2;"
                           "uid_float : float : 1.0, 2.0;"
                           "uid_double : double : 2.0, 2.0;"
                           "uid_string : string : string1, sting2;"
                           "uid_multi_int8 : multi_int8_t : -8,2#3,4;"
                           "uid_multi_int16 : multi_int16_t : -16,2#3,4;"
                           "uid_multi_int32 : multi_int32_t : -32,2#3,4;"
                           "uid_multi_int64 : multi_int64_t : -64,2#3,4;"
                           "uid_multi_uint8 : multi_uint8_t : 8,2#3,4;"
                           "uid_multi_uint16 : multi_uint16_t : 16,2#3,4;"
                           "uid_multi_uint32 : multi_uint32_t : 32,2#3,4;"
                           "uid_multi_uint64 : multi_uint64_t : 64,2#3,4;"
                           "uid_multi_float : multi_float : 1,2#3,4;"
                           "uid_multi_double : multi_double : 2,2#3,4;"
                           "uid_multi_string : multi_string : string1,string2#string3,string4";
    IndexPartitionReaderWrapperPtr idxPartiReaderWrapperPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    _funcCreator = new FunctionInterfaceCreator();
    FuncConfig fc;
    _funcCreator->init(fc, {});
    _functionProvider = new func_expression::FunctionProvider(func_expression::FunctionResource());
    _attributeExpressionCreator = new FakeAttributeExpressionCreator(_pool,
            idxPartiReaderWrapperPtr, _funcCreator, _functionProvider, NULL, NULL, NULL);
}

void AggregatorCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_funcCreator);
    DELETE_AND_SET_NULL(_functionProvider);
    DELETE_AND_SET_NULL(_attributeExpressionCreator);
}

TEST_F(AggregatorCreatorTest, testCreateDenseAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");

    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
    aggCreator._aggSamplerConfigInfo._enableDenseMode = true;

#define TEST_AGG_CREATOR(attrname, vt, isMultiValue)                    \
    {                                                                   \
        AggregateClause aggClause;                                      \
        string aggDescStr = "group_key:" + string(attrname)  + ", agg_fun:count()"; \
        ClauseParserContext ctx;                                        \
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));               \
        AggregateDescription *aggDescription = ctx.stealAggDescription(); \
        aggDescription->getGroupKeyExpr()->setMultiValue(isMultiValue); \
        aggDescription->getGroupKeyExpr()->setExprResultType(vt);       \
        aggClause.addAggDescription(aggDescription);                    \
        Aggregator *agg = aggCreator.createAggregator(&aggClause);      \
        ASSERT_TRUE(agg);                                            \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;          \
        typedef VariableTypeTraits<vt, isMultiValue>::AttrExprType AttrT; \
        typedef typename DenseMapTraits<T>::GroupMapType GroupMapType;  \
        auto normalAgg = dynamic_cast<NormalAggregator<T, AttrT, GroupMapType> *>(agg); \
        ASSERT_TRUE(normalAgg);                                         \
        const auto &functions = normalAgg->getAggregateFunctions(); \
        ASSERT_EQ((size_t)1, functions.size());              \
        ASSERT_EQ(string("count"), functions[0]->getFunctionName()); \
        DELETE_AND_SET_NULL(agg);                                         \
    }

    TEST_AGG_CREATOR("uid_int8", vt_int8, false);
    TEST_AGG_CREATOR("uid_multi_int8", vt_int8, true);
    TEST_AGG_CREATOR("uid_string", vt_string, false);
    TEST_AGG_CREATOR("uid_multi_string", vt_string, true);

    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:not_exsit_fun(), agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int32);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:sum(not_exsit_fun())";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count(), agg_filter:not_exsit_fun()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
// batchAggregator
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);

        config::AggSamplerConfigInfo info2(0,1,100,true,0);
        info2._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info2);
        agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);
    }
    {
// batchMultiAggregator
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(10,1,100,true,5);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchMultiAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);
    }
    {
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:[uid_int8, uid_int16], agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();

        auto syntaxExpr = aggDescription->getGroupKeyExpr();
        ASSERT_TRUE(syntaxExpr);
        auto multiSyntaxExpr = dynamic_cast<MultiSyntaxExpr*>(syntaxExpr);
        ASSERT_TRUE(multiSyntaxExpr);
        std::vector<SyntaxExpr *> exprs = multiSyntaxExpr->getSyntaxExprs();
        ASSERT_EQ(exprs.size(), 2);
        {
            // uid_int8
            exprs[0]->setMultiValue(false);
            exprs[0]->setExprResultType(vt_int8);
            // uid_int16
            exprs[1]->setMultiValue(false);
            exprs[1]->setExprResultType(vt_int16);
        }
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_uint64);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(agg);
        auto tupleAgg = dynamic_cast<TupleAggregator *>(agg);
        ASSERT_TRUE(tupleAgg);
        DELETE_AND_SET_NULL(agg);
    }
    {
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:[uid_multi_int8], agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();

        auto syntaxExpr = aggDescription->getGroupKeyExpr();
        ASSERT_TRUE(syntaxExpr);
        auto multiSyntaxExpr = dynamic_cast<MultiSyntaxExpr*>(syntaxExpr);
        ASSERT_TRUE(multiSyntaxExpr);
        std::vector<SyntaxExpr *> exprs = multiSyntaxExpr->getSyntaxExprs();
        ASSERT_EQ(exprs.size(), 1);
        {
            // uid_multi_int8
            exprs[0]->setMultiValue(true);
            exprs[0]->setExprResultType(vt_int8);
        }
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_uint64);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_FALSE(agg);
    }
#undef TEST_AGG_CREATOR
}

TEST_F(AggregatorCreatorTest, testCreateAggregatorUnorderedMap) {
    HA3_LOG(DEBUG, "Begin Test!");

    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
#define TEST_AGG_CREATOR(attrname, vt, isMultiValue)                    \
    {                                                                   \
        AggregateClause aggClause;                                      \
        string aggDescStr = "group_key:" + string(attrname)  + ", agg_fun:count()"; \
        ClauseParserContext ctx;                                        \
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));               \
        AggregateDescription *aggDescription = ctx.stealAggDescription(); \
        aggDescription->getGroupKeyExpr()->setMultiValue(isMultiValue); \
        aggDescription->getGroupKeyExpr()->setExprResultType(vt);       \
        aggClause.addAggDescription(aggDescription);                    \
        Aggregator *agg = aggCreator.createAggregator(&aggClause);      \
        ASSERT_TRUE(agg);                                            \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;          \
        typedef VariableTypeTraits<vt, isMultiValue>::AttrExprType AttrT; \
        typedef typename UnorderedMapTraits<T>::GroupMapType GroupTMap; \
        NormalAggregator<T, AttrT, GroupTMap> *normalAgg = dynamic_cast<NormalAggregator<T, AttrT, GroupTMap>*>(agg); \
        ASSERT_TRUE(normalAgg);                                      \
        const auto &functions = normalAgg->getAggregateFunctions(); \
        ASSERT_EQ((size_t)1, functions.size());              \
        ASSERT_EQ(string("count"), functions[0]->getFunctionName()); \
        DELETE_AND_SET_NULL(agg);                                         \
    }

    TEST_AGG_CREATOR("uid_int8", vt_int8, false);
    TEST_AGG_CREATOR("uid_multi_int8", vt_int8, true);
    TEST_AGG_CREATOR("uid_string", vt_string, false);
    TEST_AGG_CREATOR("uid_multi_string", vt_string, true);

    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:not_exsit_fun(), agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int32);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:sum(not_exsit_fun())";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count(), agg_filter:not_exsit_fun()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(!agg);
    }
    {
// batchAggregator
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);

        config::AggSamplerConfigInfo info2(0,1,100,true,0);
        info2._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info2);
        agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);
    }
    {
// batchMultiAggregator
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(10,1,100,true,5);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        aggDescStr = "group_key:uid_int8, agg_fun:count()";
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        aggDescription = ctx.stealAggDescription();
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_EQ(string("batchMultiAgg"), agg->getName());
        DELETE_AND_SET_NULL(agg);
    }
    {
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:[uid_int8, uid_int16], agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();

        auto syntaxExpr = aggDescription->getGroupKeyExpr();
        ASSERT_TRUE(syntaxExpr);
        auto multiSyntaxExpr = dynamic_cast<MultiSyntaxExpr*>(syntaxExpr);
        ASSERT_TRUE(multiSyntaxExpr);
        std::vector<SyntaxExpr *> exprs = multiSyntaxExpr->getSyntaxExprs();
        ASSERT_EQ(exprs.size(), 2);
        {
            // uid_int8
            exprs[0]->setMultiValue(false);
            exprs[0]->setExprResultType(vt_int8);
            // uid_int16
            exprs[1]->setMultiValue(false);
            exprs[1]->setExprResultType(vt_int16);
        }
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_uint64);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_TRUE(agg);
        auto tupleAgg = dynamic_cast<TupleAggregator *>(agg);
        ASSERT_TRUE(tupleAgg);
        DELETE_AND_SET_NULL(agg);
    }
    {
        AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
        config::AggSamplerConfigInfo info(0,1,100,true,10);
        info._enableDenseMode = false;
        aggCreator.setAggSamplerConfigInfo(info);
        AggregateClause aggClause;
        string aggDescStr = "group_key:[uid_multi_int8], agg_fun:count()";
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
        AggregateDescription *aggDescription = ctx.stealAggDescription();

        auto syntaxExpr = aggDescription->getGroupKeyExpr();
        ASSERT_TRUE(syntaxExpr);
        auto multiSyntaxExpr = dynamic_cast<MultiSyntaxExpr*>(syntaxExpr);
        ASSERT_TRUE(multiSyntaxExpr);
        std::vector<SyntaxExpr *> exprs = multiSyntaxExpr->getSyntaxExprs();
        ASSERT_EQ(exprs.size(), 1);
        {
            // uid_multi_int8
            exprs[0]->setMultiValue(true);
            exprs[0]->setExprResultType(vt_int8);
        }
        aggDescription->getGroupKeyExpr()->setMultiValue(false);
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_uint64);
        aggClause.addAggDescription(aggDescription);
        Aggregator *agg = aggCreator.createAggregator(&aggClause);
        ASSERT_FALSE(agg);
    }
#undef TEST_AGG_CREATOR
}


TEST_F(AggregatorCreatorTest, testAggregatorFunc) {
    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);
    aggCreator._aggSamplerConfigInfo._enableDenseMode = true;

#define CASE(func, sumExprField, ResultType, ExprType)                  \
    {                                                                   \
        AggregateClause aggClause;                                      \
        string aggDescStr = "group_key:uid_int8, agg_fun:" func "(" sumExprField ")"; \
        ClauseParserContext ctx;                                        \
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));            \
        AggregateDescription *aggDescription = ctx.stealAggDescription(); \
        aggDescription->getGroupKeyExpr()->setMultiValue(false);        \
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);  \
        aggClause.addAggDescription(aggDescription);                    \
        aggDescription->_aggFunDescriptions[0]->getSyntaxExpr()->setExprResultType(ExprType);   \
        Aggregator *agg = aggCreator.createAggregator(&aggClause);      \
        ASSERT_TRUE(agg != NULL);                                       \
        typedef typename DenseMapTraits<int8_t>::GroupMapType GroupInt8Map; \
        auto normalAgg = dynamic_cast<NormalAggregator<int8_t, int8_t, GroupInt8Map> *>(agg); \
        ASSERT_NE(nullptr, normalAgg);                                  \
        ASSERT_TRUE(normalAgg->_funVector.size() == 1);                 \
        auto aggFunc = normalAgg->_funVector[0];                        \
        ASSERT_TRUE(aggFunc->getExpr()->getType() == ExprType);         \
        ASSERT_TRUE(aggFunc->getReference()->getValueType().getBuiltinType() == ResultType); \
        DELETE_AND_SET_NULL(agg);                                       \
    }
    // test sum
    CASE("sum", "uid_int8", vt_int64, vt_int8);
    CASE("sum", "uid_int16", vt_int64, vt_int16);
    CASE("sum", "uid_int32", vt_int64, vt_int32);
    CASE("sum", "uid_int64", vt_int64, vt_int64);
    CASE("sum", "uid_uint8", vt_int64, vt_uint8);
    CASE("sum", "uid_uint16", vt_int64, vt_uint16);
    CASE("sum", "uid_uint32", vt_int64, vt_uint32);
    CASE("sum", "uid_uint64", vt_uint64, vt_uint64);
    CASE("sum", "uid_float", vt_float, vt_float);
    CASE("sum", "uid_double", vt_double, vt_double);
    // min
    CASE("min", "uid_int8", vt_int8, vt_int8);
    CASE("min", "uid_int16", vt_int16, vt_int16);
    CASE("min", "uid_int32", vt_int32, vt_int32);
    CASE("min", "uid_int64", vt_int64, vt_int64);
    CASE("min", "uid_uint8", vt_uint8, vt_uint8);
    CASE("min", "uid_uint16", vt_uint16, vt_uint16);
    CASE("min", "uid_uint32", vt_uint32, vt_uint32);
    CASE("min", "uid_uint64", vt_uint64, vt_uint64);
    CASE("min", "uid_float", vt_float, vt_float);
    CASE("min", "uid_double", vt_double, vt_double);
    // max
    CASE("max", "uid_int8", vt_int8, vt_int8);
    CASE("max", "uid_int16", vt_int16, vt_int16);
    CASE("max", "uid_int32", vt_int32, vt_int32);
    CASE("max", "uid_int64", vt_int64, vt_int64);
    CASE("max", "uid_uint8", vt_uint8, vt_uint8);
    CASE("max", "uid_uint16", vt_uint16, vt_uint16);
    CASE("max", "uid_uint32", vt_uint32, vt_uint32);
    CASE("max", "uid_uint64", vt_uint64, vt_uint64);
    CASE("max", "uid_float", vt_float, vt_float);
    CASE("max", "uid_double", vt_double, vt_double);
#undef CASE
}


TEST_F(AggregatorCreatorTest, testAggregatorFuncUnorderedMap) {
    AggregatorCreator aggCreator(_attributeExpressionCreator, _pool);

#define CASE(func, sumExprField, ResultType, ExprType)                  \
    {                                                                   \
        AggregateClause aggClause;                                      \
        string aggDescStr = "group_key:uid_int8, agg_fun:" func "(" sumExprField ")"; \
        ClauseParserContext ctx;                                        \
        ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));            \
        AggregateDescription *aggDescription = ctx.stealAggDescription(); \
        aggDescription->getGroupKeyExpr()->setMultiValue(false);        \
        aggDescription->getGroupKeyExpr()->setExprResultType(vt_int8);  \
        aggClause.addAggDescription(aggDescription);                    \
        aggDescription->_aggFunDescriptions[0]->getSyntaxExpr()->setExprResultType(ExprType);   \
        Aggregator *agg = aggCreator.createAggregator(&aggClause);      \
        ASSERT_TRUE(agg != NULL);                                       \
        typedef typename UnorderedMapTraits<int8_t>::GroupMapType GroupInt8Map; \
        auto normalAgg = dynamic_cast<NormalAggregator<int8_t, int8_t, GroupInt8Map> *>(agg); \
        ASSERT_TRUE(normalAgg != nullptr);                              \
        ASSERT_TRUE(normalAgg->_funVector.size() == 1);                 \
        auto aggFunc = normalAgg->_funVector[0];                        \
        ASSERT_TRUE(aggFunc->getExpr()->getType() == ExprType);         \
        ASSERT_TRUE(aggFunc->getReference()->getValueType().getBuiltinType() == ResultType); \
        DELETE_AND_SET_NULL(agg);                                       \
    }
    // test sum
    CASE("sum", "uid_int8", vt_int64, vt_int8);
    CASE("sum", "uid_int16", vt_int64, vt_int16);
    CASE("sum", "uid_int32", vt_int64, vt_int32);
    CASE("sum", "uid_int64", vt_int64, vt_int64);
    CASE("sum", "uid_uint8", vt_int64, vt_uint8);
    CASE("sum", "uid_uint16", vt_int64, vt_uint16);
    CASE("sum", "uid_uint32", vt_int64, vt_uint32);
    CASE("sum", "uid_uint64", vt_uint64, vt_uint64);
    CASE("sum", "uid_float", vt_float, vt_float);
    CASE("sum", "uid_double", vt_double, vt_double);
    // min
    CASE("min", "uid_int8", vt_int8, vt_int8);
    CASE("min", "uid_int16", vt_int16, vt_int16);
    CASE("min", "uid_int32", vt_int32, vt_int32);
    CASE("min", "uid_int64", vt_int64, vt_int64);
    CASE("min", "uid_uint8", vt_uint8, vt_uint8);
    CASE("min", "uid_uint16", vt_uint16, vt_uint16);
    CASE("min", "uid_uint32", vt_uint32, vt_uint32);
    CASE("min", "uid_uint64", vt_uint64, vt_uint64);
    CASE("min", "uid_float", vt_float, vt_float);
    CASE("min", "uid_double", vt_double, vt_double);
    // max
    CASE("max", "uid_int8", vt_int8, vt_int8);
    CASE("max", "uid_int16", vt_int16, vt_int16);
    CASE("max", "uid_int32", vt_int32, vt_int32);
    CASE("max", "uid_int64", vt_int64, vt_int64);
    CASE("max", "uid_uint8", vt_uint8, vt_uint8);
    CASE("max", "uid_uint16", vt_uint16, vt_uint16);
    CASE("max", "uid_uint32", vt_uint32, vt_uint32);
    CASE("max", "uid_uint64", vt_uint64, vt_uint64);
    CASE("max", "uid_float", vt_float, vt_float);
    CASE("max", "uid_double", vt_double, vt_double);
#undef CASE
}


END_HA3_NAMESPACE(search);
