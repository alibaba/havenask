#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <suez/turing/expression/framework/SyntaxExpr2AttrExpr.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeAttributeExpressionCreator);

FakeAttributeExpressionCreator::FakeAttributeExpressionCreator(
        autil::mem_pool::Pool *pool,
        const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper,
        const suez::turing::FunctionInterfaceCreator *funcCreator,
        suez::turing::FunctionProvider *functionProvider,
        const VirtualAttributeClause *virtualAttrClause,
        JoinDocIdConverterCreator *docIdConverterFactory,
        matchdoc::Reference<matchdoc::MatchDoc> *subDocRef)
    : AttributeExpressionCreator(pool, NULL, "",
                                 nullptr,
                                 TableInfoPtr(),
                                 VirtualAttributes(),
                                 NULL,
                                 suez::turing::CavaPluginManagerPtr(),
                                 NULL)
    , _attrFactory(POOL_NEW_CLASS(pool, MockAttributeExpressionFactory,
                                  indexPartitionReaderWrapper,
                                  docIdConverterFactory,
                                  subDocRef,
                                  pool))
    , _funcManager(funcCreator, functionProvider, pool)
{
    if (virtualAttrClause) {
        _convertor.initVirtualAttrs(virtualAttrClause->getVirtualAttributes());
    }
}

FakeAttributeExpressionCreator::~FakeAttributeExpressionCreator() {
    POOL_DELETE_CLASS(_attrFactory);
}


suez::turing::AttributeExpression* FakeAttributeExpressionCreator::createAtomicExpr(
        const string &attrName)
{
    SyntaxExpr2AttrExpr visitor(_attrFactory, &_funcManager, &_exprPool,
                                &_convertor, _pool);
    return visitor.createAtomicExpr(attrName);
}

suez::turing::AttributeExpression* FakeAttributeExpressionCreator::createAtomicExprWithoutPool(
        const string& attrName)
{
    auto expr = _attrFactory->createAtomicExpr(attrName);
    _exprPool.addNeedDeleteExpr(expr);
    return expr;
}

suez::turing::AttributeExpression* FakeAttributeExpressionCreator::createAttributeExpression(
        const SyntaxExpr *syntaxExpr, bool needValidate)
{
    SyntaxExpr2AttrExpr visitor(_attrFactory, &_funcManager, &_exprPool,
                                &_convertor, _pool);
    syntaxExpr->accept(&visitor);
    auto ret = visitor.stealAttrExpr();
    if (!ret) {
        HA3_LOG(WARN, "create attribute expression [%s] fail",
                syntaxExpr->getExprString().c_str());
        return NULL;
    }
    return ret;
}

void FakeAttributeExpressionCreator::resetAttrExprFactory(
        AttributeExpressionFactory *fakeFactory)
{
    POOL_DELETE_CLASS(_attrFactory);
    _attrFactory = fakeFactory;
}

END_HA3_NAMESPACE(search);
