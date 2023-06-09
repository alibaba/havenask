/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/queryparser/SearcherCacheParser.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "ha3/common/SearcherCacheClause.h"

namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;
using namespace autil;
using namespace isearch::common;

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, SearcherCacheParser);

SearcherCacheParser::SearcherCacheParser(ErrorResult *errorResult) { 
    _errorResult = errorResult;
}

SearcherCacheParser::~SearcherCacheParser() { 
}

SearcherCacheClause *SearcherCacheParser::createSearcherCacheClause() {
    return new SearcherCacheClause();
}

FilterClause* SearcherCacheParser::createFilterClause(SyntaxExpr *expr) {
    return new FilterClause(expr);
}

void SearcherCacheParser::setSearcherCacheUse(
        SearcherCacheClause *searcherCacheClause, const string &useStr)
{
    bool useFlag = false;
    if (useStr == SEARCHER_CACHE_USE_YES) {
        useFlag = true;
    } else if (useStr == SEARCHER_CACHE_USE_NO) {
        useFlag = false;
    } else {
        _errorResult->resetError(ERROR_CACHE_CLAUSE, 
                string("invalid searcher cache use flag: ") + useStr);
        return;
    }
    searcherCacheClause->setUse(useFlag);
}

void SearcherCacheParser::setSearcherCacheKey(
        SearcherCacheClause *searcherCacheClause, const string &keyStr)
{
    uint64_t keyValue = 0;
    if (!StringUtil::strToUInt64(keyStr.c_str(), keyValue)) {
        _errorResult->resetError(ERROR_CACHE_CLAUSE, 
                string("invalid searcher cache key: ") + keyStr);
        return;
    }
    searcherCacheClause->setKey(keyValue);
}

void SearcherCacheParser::setSearcherCacheCurTime(
        SearcherCacheClause *searcherCacheClause, const string &curTimeStr)
{
    uint32_t timeValue = 0;
    if (!StringUtil::strToUInt32(curTimeStr.c_str(), timeValue)) {
        _errorResult->resetError(ERROR_CACHE_CLAUSE, 
                string("invalid searcher cache cur_time: ") + curTimeStr);
        return;
    }
    searcherCacheClause->setCurrentTime(timeValue);
}

void SearcherCacheParser::setSearcherCacheDocNum(
        SearcherCacheClause *searcherCacheClause, const string &cacheDocNumStr)
{
    string errorMsg = string("invalid searcher cache doc num limit: ") + 
                      cacheDocNumStr;

    StringTokenizer tokens(cacheDocNumStr, ";", 
                           StringTokenizer::TOKEN_IGNORE_EMPTY | 
                           StringTokenizer::TOKEN_TRIM);
    if (tokens.getNumTokens() == 0) {
        _errorResult->resetError(ERROR_CACHE_CLAUSE, errorMsg);
        return;
    }
    vector<uint32_t> cacheDocNumVec;
    if (isOldCacheDocConfig(tokens)) {
        if (!parseOldCacheDocConfig(tokens, cacheDocNumVec)) {
            _errorResult->resetError(ERROR_CACHE_CLAUSE, errorMsg);
            return;
        }
    } else {
        uint32_t cacheDocNum;
        for (uint32_t i = 0; i < tokens.getNumTokens(); i++) {
            if (!StringUtil::strToUInt32(tokens[i].c_str(), cacheDocNum)) {
                _errorResult->resetError(ERROR_CACHE_CLAUSE, errorMsg);
                return;
            }
            cacheDocNumVec.push_back(cacheDocNum);
        }
    }
    sort(cacheDocNumVec.begin(), cacheDocNumVec.end());
    searcherCacheClause->setCacheDocNumVec(cacheDocNumVec);
}

void SearcherCacheParser::setSearcherCacheExpireExpr(
        SearcherCacheClause *searcherCacheClause, SyntaxExpr *expr)
{
    searcherCacheClause->setExpireExpr(expr);
}

void SearcherCacheParser::setRefreshAttributes(
        SearcherCacheClause *searcherCacheClause, 
        const string &refreshAttributeStr)
{
    string errorMsg = string("invalid refresh attributes: ") + refreshAttributeStr;

    StringTokenizer tokens(refreshAttributeStr, ";", 
                           StringTokenizer::TOKEN_IGNORE_EMPTY | 
                           StringTokenizer::TOKEN_TRIM);
    if (tokens.getNumTokens() == 0) {
        _errorResult->resetError(ERROR_CACHE_CLAUSE, errorMsg);
        return;
    }

    set<string> refreshAttributes;
    for (uint32_t i = 0; i < tokens.getNumTokens(); i++) {
        refreshAttributes.insert(tokens[i]);
    }
    searcherCacheClause->setRefreshAttributes(refreshAttributes);
}

bool SearcherCacheParser::isOldCacheDocConfig(const StringTokenizer& tokens){
    if (tokens.getNumTokens() != 3) {
        return false;
    }
    size_t pos;
    for(uint32_t i = 1; i < tokens.getNumTokens() ; i++){
        pos = tokens[i].find("#");
        if (pos == string::npos) { //old cache config: 1000#200
            return false;
        }
    }
    return true;
}

bool SearcherCacheParser::parseOldCacheDocConfig(const StringTokenizer& tokens, 
        vector<uint32_t>& cacheDocNumVec)
{
    uint32_t cacheDocNum;
    for(uint32_t i = 1; i < tokens.getNumTokens(); i++){
        if (!parseOldCacheDocNum(tokens[i], cacheDocNum)){
            return false;
        }
        cacheDocNumVec.push_back(cacheDocNum);
    }
    return true;
}

bool SearcherCacheParser::parseOldCacheDocNum(const string &str, 
        uint32_t &docNum) 
{
    StringTokenizer tokens(str, "#", 
                           StringTokenizer::TOKEN_IGNORE_EMPTY | 
                           StringTokenizer::TOKEN_TRIM);
    if (tokens.getNumTokens() != 2) {
        return false;
    }
    uint32_t first, second;
    if (!StringUtil::strToUInt32(tokens[0].c_str(), first) ||
        !StringUtil::strToUInt32(tokens[1].c_str(), second))
    {
        return false;
    }
    docNum = second;
    return true;
}

} // namespace queryparser
} // namespace isearch

