#ifndef UDF_PLUGINS__H
#define UDF_PLUGINS__H
 
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/SyntaxExpressionFactory.h>
#include <ha3/config/ResourceReader.h>
 
namespace pluginplatform {
namespace udf_plugins {
 
class FunctionPluginFactory : public isearch::func_expression::SyntaxExpressionFactory
{
public:
    FunctionPluginFactory();
    ~FunctionPluginFactory();
public:
    /* override */ bool init(const KeyValueMap &parameters);
    /* override */ bool registeFunctions();
private:
    std::set<std::string> _funcNames;
private:
    HA3_LOG_DECLARE();
};
 
HA3_TYPEDEF_PTR(FunctionPluginFactory);
 
}}
 
#endif //UDF_PLUGINS__H