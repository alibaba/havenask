#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/SyntaxExpressionFactory.h>

namespace pluginplatform {
namespace function_plugins {

class PostageFunctionFactory : public isearch::func_expression::SyntaxExpressionFactory
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

}}
