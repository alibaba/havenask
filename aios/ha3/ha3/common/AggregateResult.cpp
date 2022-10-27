#include <string>
#include <sstream>
#include <ha3/common/AggregateResult.h>

using namespace autil::legacy;
using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AggregateResult);

AggregateResult::AggregateResult() {
}

AggregateResult::~AggregateResult() {
    if (_allocatorPtr) {
        for (auto doc : _matchdocVec) {
            _allocatorPtr->deallocate(doc);
        }
    }
    _exprValue2AggResult.clear();
    _matchdocVec.clear();
}

const string& AggregateResult::getAggFunName(uint32_t funOffset) const {
    if (funOffset < _funNames.size()) {
        return _funNames[funOffset];
    }

    const static string sNullString;
    return sNullString;
}

std::string AggregateResult::getAggFunDesStr(uint32_t i) const {
    if (i >= _funNames.size() || i >= _funParameters.size()) {
        return "";
    }

    return _funNames[i] + "(" + _funParameters[i] + ")";
}

const vector<string>& AggregateResult::getAggFunNames() const {
    return _funNames;
}

uint32_t AggregateResult::getAggFunCount() const {
    return _funNames.size();
}

AggregateResult::AggExprValueMap& AggregateResult::getAggExprValueMap() {
    return _exprValue2AggResult;
}

uint32_t AggregateResult::getAggExprValueCount() const {
    return _matchdocVec.size();
}

matchdoc::MatchDoc AggregateResult::getAggFunResults(
        const AggExprValue &value) const
{
    AggExprValueMap::const_iterator it = _exprValue2AggResult.find(value);
    if (it != _exprValue2AggResult.end()) {
        return it->second;
    }
    return matchdoc::INVALID_MATCHDOC;
}

void AggregateResult::addAggFunName(const std::string &functionName) {
    _funNames.push_back(functionName);
}

void AggregateResult::addAggFunParameter(const std::string &funParameter) {
    _funParameters.push_back(funParameter);
}

void AggregateResult::addAggFunResults(matchdoc::MatchDoc aggFunResults) {
    _matchdocVec.push_back(aggFunResults);
}

void AggregateResult::addAggFunResults(const std::string &key,
                                       matchdoc::MatchDoc aggFunResults)
{
    _matchdocVec.push_back(aggFunResults);
    _exprValue2AggResult.insert(make_pair(key, aggFunResults));
}

void AggregateResult::serialize(autil::DataBuffer &dataBuffer) const {
    bool bExisted = (_allocatorPtr != NULL);
    dataBuffer.writeBytes(&bExisted, sizeof(bExisted));
    if (!bExisted) {
        return;
    }
    _allocatorPtr->extend(); // empty result, Aggregator may not extend()
    _allocatorPtr->setSortRefFlag(false);
    _allocatorPtr->serialize(dataBuffer, _matchdocVec, SL_CACHE);
    dataBuffer.write(_funNames);
    dataBuffer.write(_funParameters);
    dataBuffer.write(_groupExprStr);
}

void AggregateResult::deserialize(autil::DataBuffer &dataBuffer,
                                  autil::mem_pool::Pool *pool)
{
    bool bExisted = false;
    dataBuffer.readBytes(&bExisted, sizeof(bExisted));
    if (!bExisted) {
        return;
    }
    _allocatorPtr.reset(new matchdoc::MatchDocAllocator(pool));
    assert(_allocatorPtr != NULL);
    _allocatorPtr->deserialize(dataBuffer, _matchdocVec);
    dataBuffer.read(_funNames);
    dataBuffer.read(_funParameters);
    dataBuffer.read(_groupExprStr);
}

void AggregateResult::constructGroupValueMap() {
    _exprValue2AggResult.clear();
    if (_matchdocVec.size() > 0) {
        auto ref = _allocatorPtr->findReferenceWithoutType(common::GROUP_KEY_REF);
        assert(ref);
        for (auto doc : _matchdocVec) {
            _exprValue2AggResult[ref->toString(doc)] = doc;
        }
    }
}

END_HA3_NAMESPACE(common);
