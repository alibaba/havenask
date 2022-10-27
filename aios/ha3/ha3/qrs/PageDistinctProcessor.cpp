#include <ha3/qrs/PageDistinctProcessor.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <ha3/queryparser/RequestParser.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, PageDistinctProcessor);

const string PageDistinctProcessor::PAGE_DIST_PROCESSOR_NAME = "BuildInPageDistinctProcessor";
const string PageDistinctProcessor::PAGE_DIST_PARAM_PAGE_SIZE = "page_size";
const string PageDistinctProcessor::PAGE_DIST_PARAM_DIST_COUNT = "dist_count";
const string PageDistinctProcessor::PAGE_DIST_PARAM_DIST_KEY = "dist_key";
const string PageDistinctProcessor::PAGE_DIST_PARAM_GRADE_THRESHOLDS = "grade";
const string PageDistinctProcessor::GRADE_THRESHOLDS_SEP = "|";

PageDistinctProcessor::PageDistinctProcessor() { 
}


PageDistinctProcessor::~PageDistinctProcessor() { 
}

void PageDistinctProcessor::process(RequestPtr &requestPtr,
                                    ResultPtr &resultPtr) 
{
    if (requestPtr == NULL) {
        return;
    }

    ErrorResult errorResult;
    PageDistParam pageDistParam;
    if (!getPageDistParams(requestPtr, pageDistParam, errorResult)) {
        resultPtr.reset(new Result);
        resultPtr->addErrorResult(errorResult);
        HA3_LOG(WARN,"%s", errorResult.getErrorMsg().c_str());
        return;
    }

    set<string> attrNames;
    AttributeClause *attrClause = requestPtr->getAttributeClause();
    if (attrClause) {
        attrNames = attrClause->getAttributeNames();
    }

    if (!resetRequest(requestPtr, pageDistParam, errorResult)) {
        resultPtr.reset(new Result);
        resultPtr->addErrorResult(errorResult);
        HA3_LOG(WARN,"%s", errorResult.getErrorMsg().c_str());
        return;
    }
    
    QrsProcessor::process(requestPtr, resultPtr);

    selectPages(pageDistParam, resultPtr);
    restoreRequest(requestPtr, pageDistParam, attrNames);
}

QrsProcessorPtr PageDistinctProcessor::clone() {
    return QrsProcessorPtr(new PageDistinctProcessor(*this));
}

PageDistinctSelectorPtr PageDistinctProcessor::createPageDistinctSelector(
        const PageDistParam &pageDistParam, const HitPtr &hitPtr) const
{
    if (hitPtr == NULL) {
        return PageDistinctSelectorPtr();
    }

    AttributeItemPtr attrItemPtr = hitPtr->getAttribute(pageDistParam.distKey);
    if (attrItemPtr == NULL) {
        return PageDistinctSelectorPtr();
    }

#ifndef CREATE_PAGEDISTSELECTOR_CASE
#define CREATE_PAGEDISTSELECTOR_CASE(vt)                                \
    case vt:                                                            \
        return PageDistinctSelectorPtr(                                 \
                new PageDistinctSelectorTyped<VariableTypeTraits<vt, false>::AttrExprType>( \
                        pageDistParam.pgHitNum, pageDistParam.distKey,  \
                        pageDistParam.distCount,                        \
                        pageDistParam.sortFlags,                        \
                        pageDistParam.gradeThresholds));

    auto vt = attrItemPtr->getType();
    switch (vt) {
        CREATE_PAGEDISTSELECTOR_CASE(vt_int8);
        CREATE_PAGEDISTSELECTOR_CASE(vt_int16);
        CREATE_PAGEDISTSELECTOR_CASE(vt_int32);
        CREATE_PAGEDISTSELECTOR_CASE(vt_int64);
        CREATE_PAGEDISTSELECTOR_CASE(vt_uint8);
        CREATE_PAGEDISTSELECTOR_CASE(vt_uint16);
        CREATE_PAGEDISTSELECTOR_CASE(vt_uint32);
        CREATE_PAGEDISTSELECTOR_CASE(vt_uint64);
    default:
        HA3_LOG(WARN, "invalid variable type: [%s]", vt2TypeName(vt).c_str());
        return PageDistinctSelectorPtr();
    }
#undef CREATE_PAGEDISTSELECTOR_CASE
#endif
    return PageDistinctSelectorPtr();
}

void PageDistinctProcessor::getSortFlags(const RequestPtr &requestPtr,
        vector<bool> &sortFlags) const
{
    sortFlags.clear();
    SortClause *sortClause = requestPtr->getSortClause();
    if (sortClause == NULL) {
        sortFlags.resize(1, false);
        return;
    }
    
    const vector<SortDescription*> &sortClauses = 
        sortClause->getSortDescriptions();
    sortFlags.reserve(sortClauses.size());
    for (vector<SortDescription*>::const_iterator it = sortClauses.begin();
         it != sortClauses.end(); ++it) 
    {
        const SortDescription *sortDesc = *it;
        sortFlags.push_back(sortDesc->getSortAscendFlag());
    }
    if (sortFlags.empty()) {
        sortFlags.resize(1, false);
    }
}

bool PageDistinctProcessor::getKVPairValue(const ConfigClause *configClause,
        const string &key, string &value, 
        ErrorResult &errorResult) const
{
    value = configClause->getKVPairValue(key);
    StringUtil::trim(value);
    if (value == "") {
        string errorMsg = string("not specify the [") + key + 
                          "] key in kvpair of ConfigClause";
        errorResult.resetError(ERROR_GENERAL, errorMsg);
        return false;
    }
    return true;
}

bool PageDistinctProcessor::getKVPairUint32Value(
        const ConfigClause *configClause,
        const string &key, uint32_t &value, 
        ErrorResult &errorResult) const
{
    string strValue;
    if (!getKVPairValue(configClause, key, strValue, errorResult))
    {
        return false;
    }
    
    if (!StringUtil::strToUInt32(strValue.c_str(), value)) {
        string errorMsg = string("trans ") + key + 
                          " from string [" + strValue + 
                          "] to uint32_t failed";
        errorResult.resetError(ERROR_GENERAL, errorMsg);
        return false;
    }

    return true;
}

bool PageDistinctProcessor::getPageHitNum(
        const ConfigClause *configClause,
        uint32_t &pgHitNum, 
        ErrorResult &errorResult) const
{

    return getKVPairUint32Value(configClause, 
                                PAGE_DIST_PARAM_PAGE_SIZE,
                                pgHitNum, errorResult);
}

bool PageDistinctProcessor::getDistCount(
        const ConfigClause *configClause,
        uint32_t &distCount, 
        ErrorResult &errorResult) const
{
    return getKVPairUint32Value(configClause, 
                                PAGE_DIST_PARAM_DIST_COUNT,
                                distCount, errorResult);
}

bool PageDistinctProcessor::getDistKey(const ConfigClause *configClause,
                                       string &distKey, 
                                       ErrorResult &errorResult) const
{
    return getKVPairValue(configClause, PAGE_DIST_PARAM_DIST_KEY, 
                          distKey, errorResult);
}

bool PageDistinctProcessor::getGradeThresholds(
        const ConfigClause *configClause,
        vector<double> &gradeThresholds,
        string &originalGradeStr,
        ErrorResult &errorResult) const
{
    gradeThresholds.clear();
    const map<string, string> &kvMap = configClause->getKVPairs();
    const map<string, string>::const_iterator it = 
        kvMap.find(PAGE_DIST_PARAM_GRADE_THRESHOLDS);
    if (it == kvMap.end()) 
    {
        return true;
    }

    string errorMsg;
    string strValue = it->second;
    StringTokenizer st(strValue, GRADE_THRESHOLDS_SEP, 
                       StringTokenizer::TOKEN_TRIM |
                       StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() == 0) {
        errorMsg = "the grade threshold is empty";
        errorResult.resetError(ERROR_GENERAL, errorMsg);
        return false;
    }

    for (StringTokenizer::Iterator gradeIt = st.begin();
         gradeIt != st.end(); ++gradeIt)
    {
        double gradeValue;
        if (!StringUtil::strToDouble((*gradeIt).c_str(), gradeValue)) {
            errorMsg = string("trans grade value form string [") + 
                       *gradeIt + "] to double failed";
            errorResult.resetError(ERROR_GENERAL, errorMsg);
            return false;
        }
        gradeThresholds.push_back(gradeValue);
    }

    originalGradeStr = strValue;
    
    return true;
}

bool PageDistinctProcessor::getPageDistParams(const RequestPtr &requestPtr,
        PageDistParam &pageDistParam,
        ErrorResult &errorResult) const
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (configClause == NULL) {
        errorResult.resetError(ERROR_GENERAL, "ConfigClause is null");
        return false;
    }

    if (!getPageHitNum(configClause, pageDistParam.pgHitNum, errorResult) ||
        !getDistKey(configClause, pageDistParam.distKey, errorResult) ||
        !getDistCount(configClause, pageDistParam.distCount, errorResult) ||
        !getGradeThresholds(configClause, pageDistParam.gradeThresholds, 
                            pageDistParam.originalGradeStr, errorResult))
    {
        return false;
    }

    pageDistParam.startOffset = configClause->getStartOffset();
    pageDistParam.hitCount = configClause->getHitCount();
    getSortFlags(requestPtr, pageDistParam.sortFlags);
    return true;
}

bool PageDistinctProcessor::resetRequest(RequestPtr requestPtr, 
        const PageDistParam &pageDistParam, 
        ErrorResult &errorResult) const
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    configClause->setStartOffset(0);

    uint32_t pageCount = calcWholePageCount(pageDistParam);
    uint32_t hitCount = pageDistParam.pgHitNum * pageCount * 2;
    configClause->setHitCount(hitCount);

    if (!setDistinctClause(requestPtr, pageDistParam, errorResult)) {
        return false;
    }
    
    setAttributeClause(requestPtr, pageDistParam);
    return true;
}

bool PageDistinctProcessor::setDistinctClause(RequestPtr requestPtr, 
        const PageDistParam &pageDistParam, ErrorResult &errorResult) const
{
    string errorMsg;
    if (requestPtr->getDistinctClause()) {
        errorMsg = "page distinct confict with the distinct clause "
                   "in the original request";
        errorResult.resetError(ERROR_GENERAL, errorMsg);
        return false;
    }

    string distClauseStr = getDistClauseString(pageDistParam);
    DistinctClause *distClause = new DistinctClause;
    distClause->setOriginalString(distClauseStr);
    requestPtr->setDistinctClause(distClause);

    RequestParser requestParser;
    if (!requestParser.parseDistinctClause(requestPtr)) {
        errorResult = requestParser.getErrorResult();
        return false;
    }
    return true;
}

void PageDistinctProcessor::setAttributeClause(common::RequestPtr requestPtr, 
        const PageDistParam &pageDistParam) const 
{
    AttributeClause *attrClause = requestPtr->getAttributeClause();
    if (!attrClause) {
        attrClause = new AttributeClause;
        requestPtr->setAttributeClause(attrClause);
    } 
    attrClause->addAttribute(pageDistParam.distKey);
}

std::string PageDistinctProcessor::getDistClauseString(
        const PageDistParam &pageDistParam) const
{
    uint32_t pageCount = calcWholePageCount(pageDistParam);
    uint32_t distCount = pageDistParam.distCount * pageCount;

    stringstream ss;
    ss << DISTINCT_NONE_DIST << DISTINCT_CLAUSE_SEPERATOR
       << DISTINCT_KEY << DISTINCT_KV_SEPERATOR << pageDistParam.distKey 
       << DISTINCT_DESCRIPTION_SEPERATOR
       << DISTINCT_COUNT << DISTINCT_KV_SEPERATOR << distCount;
    
    if (pageDistParam.gradeThresholds.size() == 0) {
        return ss.str();
    }

    ss << DISTINCT_DESCRIPTION_SEPERATOR;
    ss << DISTINCT_GRADE << DISTINCT_KV_SEPERATOR;
    ss << pageDistParam.originalGradeStr;

    return ss.str();
}

uint32_t PageDistinctProcessor::calcWholePageCount(
        const PageDistParam &pageDistParam) const
{
    uint32_t totalHitCount = pageDistParam.startOffset + 
                             pageDistParam.hitCount;

    return ((totalHitCount % pageDistParam.pgHitNum) ?
            (totalHitCount / pageDistParam.pgHitNum + 1) :
            (totalHitCount / pageDistParam.pgHitNum));
}

void PageDistinctProcessor::selectPages(const PageDistParam &pageDistParam, 
                                        ResultPtr &resultPtr)
{
    ErrorResult errorResult;
    if (resultPtr == NULL || resultPtr->hasError()) {
        return;
    }

    Hits *hits = resultPtr->getHits();
    if (hits == NULL) {
        return;
    }

    const vector<HitPtr> &hitVec = hits->getHitVec();
    if (hitVec.size() == 0) {
        return;
    }
    
    PageDistinctSelectorPtr pageDistSelectorPtr = 
        createPageDistinctSelector(pageDistParam, hitVec[0]);
    if (pageDistSelectorPtr == NULL) {
        HA3_LOG(WARN, "create PageDistinctSelector failed");
        errorResult.resetError(ERROR_GENERAL, 
                               "create PageDistinctSelector failed");
        resultPtr->addErrorResult(errorResult);
        return;
    }
    
    vector<HitPtr> pageHits;
    pageDistSelectorPtr->selectPages(hitVec, 
            pageDistParam.startOffset, 
            pageDistParam.hitCount, pageHits);

    hits->setHitVec(pageHits);
}

void PageDistinctProcessor::restoreRequest(RequestPtr requestPtr, 
        const PageDistParam &pageDistParam, 
        const set<string> &attrNames)
{
    ConfigClause *configClause = requestPtr->getConfigClause();
    if (configClause) {
        configClause->setStartOffset(pageDistParam.startOffset);
        configClause->setHitCount(pageDistParam.hitCount);
    }
    requestPtr->setDistinctClause(NULL);
    
    if (attrNames.empty()) {
        requestPtr->setAttributeClause(NULL);
    } else {
        assert(requestPtr->getAttributeClause());
        requestPtr->getAttributeClause()->setAttributeNames(
                attrNames.begin(), attrNames.end());
    }
}

END_HA3_NAMESPACE(qrs);

