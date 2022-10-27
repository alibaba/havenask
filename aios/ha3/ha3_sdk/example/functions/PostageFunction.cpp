#include <ha3_sdk/example/functions/PostageFunction.h>
#include <ha3/search/ArgumentAttributeExpression.h>

using namespace std;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(func_expression);
HA3_LOG_SETUP(func_expression, PostageFunction);


bool PostageFunctionCreatorImpl::init(const KeyValueMap &funcParameter,
                                      const ResourceReaderPtr &resourceReader)
{
    string fileContext;
    if (!resourceReader->getFileContent(fileContext, "postage_map")) {
        return false;
    }
    istringstream iss(fileContext);
    pair<int32_t, int32_t> post;
    double price;
    double distance;
    while (iss >> post.first >> post.second >> price >> distance) {
        _postageMap[post] = make_pair(price, distance);
    }
    return true;
}

FunctionInterface *PostageFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec)
{
    if (funcSubExprVec.size() != 2) {
        return NULL;
    }
    AttributeExpressionTyped<int32_t> *sellerCityAttrExpr =
        dynamic_cast<AttributeExpressionTyped<int32_t> *>(funcSubExprVec[0]);
    if (!sellerCityAttrExpr) {
        return NULL;
    }
    ArgumentAttributeExpression *arg =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[1]);
    if (!arg) {
        return NULL;
    }

    int32_t buyerCity;
    if (!arg->getConstValue<int32_t>(buyerCity)) {
        return NULL;
    }

    return new PostageFunction(sellerCityAttrExpr, buyerCity, _postageMap);
}

/////////////////////////////////////////////////////////////////////////

PostageFunction::PostageFunction(AttributeExpressionTyped<int32_t> *sellerCityAttrExpr,
                                 int32_t buyerCity, const PostageMap &postageMap)
    : _sellerCityAttrExpr(sellerCityAttrExpr)
    , _buyerCity(buyerCity)
    , _postageMap(postageMap)
    , _distanceRef(NULL)
    , _minPostage(NULL)
{
}

PostageFunction::~PostageFunction() {
}

bool PostageFunction::beginRequest(suez::turing::FunctionProvider *provider) {
    HA3_NS(func_expression)::FunctionProvider *funcProvider =
        dynamic_cast<HA3_NS(func_expression)::FunctionProvider *>(provider);
    _distanceRef = funcProvider->declareVariable<double>("distance", true);
    if (!_distanceRef) {
        return false;
    }
    _minPostage = funcProvider->getDataProvider()->declareGlobalVariable<double>("min_postage", true);
    if (!_minPostage) {
        return false;
    }
    *_minPostage = numeric_limits<double>::max();
    return true;
}

double PostageFunction::evaluate(matchdoc::MatchDoc matchDoc) {
    (void)_sellerCityAttrExpr->evaluate(matchDoc);
    int32_t sellerCity = _sellerCityAttrExpr->getValue(matchDoc);
    pair<int32_t, int32_t> post(sellerCity, _buyerCity);
    PostageMap::const_iterator it = _postageMap.find(post);
    double postage = 20.0; // default
    double distance = 10000.0; //default
    if (it != _postageMap.end()) {
        postage = it->second.first;
        distance = it->second.second;
    }
    if (*_minPostage > postage) {
        *_minPostage = postage;
    }
    _distanceRef->set(matchDoc, distance);
    return postage;
}

END_HA3_NAMESPACE(func_expression);
