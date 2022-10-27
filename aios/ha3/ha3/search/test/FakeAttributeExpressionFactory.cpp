#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeAttributeExpressionFactory);

FakeAttributeExpressionFactory::FakeAttributeExpressionFactory(
        autil::mem_pool::Pool *pool)
    : AttributeExpressionFactory("mainTable", nullptr, NULL, pool, NULL)
{
    _pool = pool;
}

FakeAttributeExpressionFactory::FakeAttributeExpressionFactory(
        const string &name, const string &values,
        autil::mem_pool::Pool *pool)
    : AttributeExpressionFactory("mainTable", nullptr, NULL, pool, NULL)
{
    setInt32Attribute(name, values);
    _pool = pool;
}

FakeAttributeExpressionFactory::~FakeAttributeExpressionFactory() {
}


AttributeExpression* FakeAttributeExpressionFactory::createAtomicExpr(
        const std::string &attributeName)
{
    map<string, vector<int32_t> >::const_iterator it
        = _int32Attributes.find(attributeName);
    if (it != _int32Attributes.end()) {
        return POOL_NEW_CLASS(_pool, FakeAttributeExpression<int32_t>,
                              attributeName, &(it->second));
    } else {
        return NULL;
    }
}

void FakeAttributeExpressionFactory::setInt32Attribute(
        const std::string &name, const std::string &values)
{
    const vector<string> &strs = StringUtil::split(values, ",");
    vector<int32_t> ints;
    for (size_t i = 0; i < strs.size(); i++) {
        int32_t v;
        if (StringUtil::strToInt32(strs[i].c_str(), v)) {
            ints.push_back(v);
        }
    }
    _int32Attributes[name] = ints;
}


END_HA3_NAMESPACE(search);
