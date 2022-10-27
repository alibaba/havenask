#include <ha3/common/AggResultReader.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;

USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AggResultReader);

AggResultReader::AggResultReader(const AggregateResultsPtr &aggResults)
    : _aggResults(aggResults)
{
    _constructedMap.resize(aggResults ? aggResults->size() : 0, false);
}

AggResultReader::~AggResultReader() { 
}

AggResultReader::BasePair AggResultReader::getAggResult(
        const string &key, const string &funDes)
{
    BasePair ret(NULL, NULL);

    if (!_aggResults) {
        return ret;
    }

    ClauseParserContext ctx;
    string keyExprStr = ctx.getNormalizedExprStr(key);
    string funDesExprStr = ctx.getNormalizedExprStr(funDes);

    if (keyExprStr.empty()) { 
        HA3_LOG(WARN, "invalid key [%s]", key.c_str());
        return ret;
    }

    if (funDesExprStr.empty()) {
        HA3_LOG(WARN, "invalid funDes [%s]", funDes.c_str());
        return ret;
    }
    
    for (size_t i = 0; i < _aggResults->size(); ++i) {
        const AggregateResultPtr &resultPtr = (*_aggResults)[i];
        if (!resultPtr) {
            continue;
        }
        if (resultPtr->_groupExprStr != keyExprStr) {
            continue;
        }
        if (!_constructedMap[i]) {
            resultPtr->constructGroupValueMap();
            _constructedMap[i] = true;
        }
        
        uint32_t funCount = resultPtr->getAggFunCount();
        for (uint32_t i = 0; i < funCount; ++i) {
            if (funDesExprStr == resultPtr->getAggFunDesStr(i)) {
                ret.first = &(resultPtr->_exprValue2AggResult);
                ret.second = resultPtr->getAggFunResultReferBase(i);
                return ret;
            }
        }
    }
    
    return ret;
}

END_HA3_NAMESPACE(common);

