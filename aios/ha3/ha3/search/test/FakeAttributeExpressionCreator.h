#ifndef ISEARCH_ATTRIBUTEEXPRESSIONCREATOR_H
#define ISEARCH_ATTRIBUTEEXPRESSIONCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/function/FunctionManager.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <indexlib/partition/index_partition_reader.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/search/test/MockAttributeExpressionFactory.h>
#include <suez/turing/expression/util/VirtualAttrConvertor.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class FakeAttributeExpressionCreator : public suez::turing::AttributeExpressionCreator
{
public:
    FakeAttributeExpressionCreator(
            autil::mem_pool::Pool *pool,
            const IndexPartitionReaderWrapperPtr &indexPartReaderWrapper,
            const suez::turing::FunctionInterfaceCreator *funcCreator,
            suez::turing::FunctionProvider *functionProvider,
            const common::VirtualAttributeClause *virtualAttrClause,
            suez::turing::JoinDocIdConverterCreator *docIdConverterFactory,
            matchdoc::Reference<matchdoc::MatchDoc> *subDocRef);
    ~FakeAttributeExpressionCreator();
private:
    FakeAttributeExpressionCreator(const FakeAttributeExpressionCreator &);
    FakeAttributeExpressionCreator& operator=(const FakeAttributeExpressionCreator &);

public:
    // virtual for fake
    virtual suez::turing::AttributeExpression* createAtomicExpr(
            const std::string &attrName);
    
    virtual suez::turing::AttributeExpression* createAttributeExpression(
            const suez::turing::SyntaxExpr *syntaxExpr, bool needValidate = false);

    virtual suez::turing::AttributeExpression* createAtomicExprWithoutPool(const std::string& attrName);
private:
    // for test
    virtual void resetAttrExprFactory(suez::turing::AttributeExpressionFactory *fakeFactory);
private:
    suez::turing::AttributeExpressionFactory *_attrFactory;
    suez::turing::FunctionManager _funcManager;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeAttributeExpressionCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ATTRIBUTEEXPRESSIONCREATOR_H
