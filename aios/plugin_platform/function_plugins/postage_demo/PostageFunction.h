#ifndef ISEARCH_POSTAGEFUNCTION_H
#define ISEARCH_POSTAGEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/func_expression/FunctionInterface.h>
#include <ha3/func_expression/FunctionCreator.h>
#include <ha3/common/DataProvider.h>
#include <ha3/search/AttributeExpression.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(func_expression);

class PostageFunction : public FunctionInterfaceTyped<double>
{
public:
    typedef std::map<std::pair<int32_t, int32_t>, std::pair<double, double> > PostageMap;
public:
    PostageFunction(HA3_NS(search)::AttributeExpressionTyped<int32_t> *sellerCityAttrExpr,
                  int32_t buyerCity, const PostageMap &postageMap);
    ~PostageFunction();
private:
    PostageFunction(const PostageFunction &);
    PostageFunction& operator=(const PostageFunction &);
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) override;
    double evaluate(matchdoc::MatchDoc matchDoc) override;
    void endRequest() override {
    }
private:
    HA3_NS(search)::AttributeExpressionTyped<int32_t> *_sellerCityAttrExpr;
    int32_t _buyerCity;
    const PostageMap &_postageMap;
    matchdoc::Reference<double> *_distanceRef;
    double *_minPostage;
private:
    HA3_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////

DECLARE_FUNCTION_CREATOR(PostageFunction, "postage", 2);
class PostageFunctionCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(PostageFunction)
{
public:
    typedef PostageFunction::PostageMap PostageMap;
public:
    /* override */ bool init(const KeyValueMap &funcParameter,
                                 const HA3_NS(config)::ResourceReaderPtr &resourceReader);

    /* override */ FunctionInterface *createFunction(const FunctionSubExprVec& funcSubExprVec);
private:
    PostageMap _postageMap;
};

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_POSTAGEFUNCTION_H
