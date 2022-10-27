#include <ha3_sdk/testlib/sorter/SorterTestHelper.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, SorterTestHelper);

SorterTestHelper::SorterTestHelper(const std::string &testPath) : TestHelperBase(testPath) { 
    _scoreExpression = NULL;
    _cmpCreator = NULL;
    _sortExprCreator = NULL;
    _provider = NULL;
}

SorterTestHelper::~SorterTestHelper() { 
    POOL_DELETE_CLASS(_cmpCreator);
    POOL_DELETE_CLASS(_sortExprCreator);
    POOL_DELETE_CLASS(_provider);
}

bool SorterTestHelper::init(TestParamBase *param) {
    _param = *(common::SorterTestParam *)param;

    if (!_param.scoreStr.empty()) {
        string scoreAttribute = string(SCORE_REF) + ":double:" + _param.scoreStr;
        _param.fakeIndex.attributes += ";";
        _param.fakeIndex.attributes += scoreAttribute;
    }
    
    if (!TestHelperBase::init(&_param)) {
        return false;
    }

    SorterResource resource;
    fillSearchPluginResource(&resource);

    // sorter specific resource
    _scoreExpression = _attrExprCreator->createAtomicExpr(SCORE_REF);
    resource.scoreExpression = _scoreExpression;
    resource.resultSourceNum = _param.resultSourceNum;
    resource.requiredTopk = &(_param.requiredTopK);
    _cmpCreator = POOL_NEW_CLASS(_pool, ComparatorCreator, _pool);
    resource.comparatorCreator = _cmpCreator;
    _sortExprCreator = POOL_NEW_CLASS(_pool, SortExpressionCreator, 
            (AttributeExpressionCreator *)_attrExprCreator, 
            NULL, _mdAllocatorPtr.get(), _errorResult, _pool);
    resource.sortExprCreator = _sortExprCreator;
    resource.location = fromString(_param.location);
    resource.globalVariables = NULL; // TODO: support global variables

    _provider = POOL_NEW_CLASS(_pool, HA3_NS(sorter)::SorterProvider, resource);
    _providerBase = _provider;

    return _provider != NULL;
}

SorterLocation SorterTestHelper::fromString(const string &locationStr) {
    SorterLocation location = SL_UNKNOWN;
    if (locationStr == "searcher") {
        location = SL_SEARCHER;
    } else if (locationStr == "proxy") {
        location = SL_PROXY;
    } else if (locationStr == "qrs") {
        location = SL_QRS;
    } 
    return location;
}

bool SorterTestHelper::beginRequest(Sorter *sorter) {
    return true;
}

bool SorterTestHelper::initScoreAttrExpression() {
    bool ret = true;
    if (_scoreExpression) {
        ret = _scoreExpression->allocate(_mdAllocatorPtr.get());
    }
    if (!ret) {
        return false;
    }
    return true;
}

bool SorterTestHelper::beginSort(Sorter *sorter) {
    _sorterWrap.reset(new SorterWrapper(sorter));
    if (!initScoreAttrExpression()) {
        return false;
    }
    return _sorterWrap->beginSort(_provider);
}

bool SorterTestHelper::sort(Sorter *sorter) {
    // prepare matchDocs
    allocateMatchDocs(_param.docIdStr, _matchDocs);
    if (_matchDataRef != NULL && !_param.matchDataStr.empty()) {
        fillMatchDatas(_param.matchDataStr, _matchDocs);
    }
    // make sure all _needEvaluateAttrExprs evaluated
    SortParam param(_pool);
    param.matchDocs.insert(param.matchDocs.end(), _matchDocs.begin(), _matchDocs.end());

    if (!evaluateScoreAttrExpr(_scoreExpression, param.matchDocs)) {
        return false;
    }

    // sort
    param.requiredTopK = _param.requiredTopK;
    param.totalMatchCount = _param.totalMatchCount;
    param.actualMatchCount = _param.actualMatchCount;
    _sorterWrap->sort(param);

    // check result
    vector<docid_t> expectedDocIds;
    StringUtil::fromString(_param.expectedDocIdStr, expectedDocIds, ",");
    if (expectedDocIds.size() != _matchDocs.size()) {
        return false;
    }
    bool allEqual = true;
    for (size_t i = 0; i < param.matchDocs.size(); i++) {
        if (expectedDocIds[i] != param.matchDocs[i].getDocId()) {
            allEqual = false;
            break;
        }
    }
    return allEqual;
}

template <typename T>
bool SorterTestHelper::evaluateScoreAttrExpr(AttributeExpression *attrExpr, 
        const T &matchDocs)
{
    if (!attrExpr) {
        return true;
    }
    for (size_t i = 0; i < matchDocs.size(); i++) {
        if (!attrExpr->evaluate(matchDocs[i])) {
            return false;
        }
    }
    return true;
}

bool SorterTestHelper::endSort(Sorter *sorter) {
    _sorterWrap->endSort();
    return true;
}

bool SorterTestHelper::endRequest(Sorter *sorter) {
    return true;
}

END_HA3_NAMESPACE(sorter);

