#ifndef ISEARCH_POSTAGEFUNCTIONFACTORY_H
#define ISEARCH_POSTAGEFUNCTIONFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/SyntaxExpressionFactory.h>

BEGIN_HA3_NAMESPACE(func_expression);

class PostageFunctionFactory : public SyntaxExpressionFactory
{
public:
    PostageFunctionFactory();
    ~PostageFunctionFactory();
private:
    PostageFunctionFactory(const PostageFunctionFactory &);
    PostageFunctionFactory& operator=(const PostageFunctionFactory &);
public:
    /* override */ bool registeFunctions();
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PostageFunctionFactory);

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_POSTAGEFUNCTIONFACTORY_H
