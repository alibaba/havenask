#ifndef ISEARCH_FAKEATTRIBUTEEXPRESSIONFACTORY_H
#define ISEARCH_FAKEATTRIBUTEEXPRESSIONFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpressionFactory.h>

BEGIN_HA3_NAMESPACE(search);

class FakeAttributeExpressionFactory : public suez::turing::AttributeExpressionFactory
{
public:
    FakeAttributeExpressionFactory(
            autil::mem_pool::Pool *pool);
    FakeAttributeExpressionFactory(
            const std::string &name,
            const std::string &values,
            autil::mem_pool::Pool *pool);
    
    /* override */ ~FakeAttributeExpressionFactory();
public:
    /* override */
    suez::turing::AttributeExpression* createAtomicExpr(const std::string &attributeName);
public:
    // for test.
    void setInt32Attribute(const std::string &name, const std::string &values);
private:
    std::map<std::string, std::vector<int32_t> > _int32Attributes;
    autil::mem_pool::Pool *_pool;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEATTRIBUTEEXPRESSIONFACTORY_H
