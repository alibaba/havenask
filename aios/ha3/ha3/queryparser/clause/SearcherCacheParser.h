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
#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class StringTokenizer;
}  // namespace autil
namespace isearch {
namespace common {
class ErrorResult;
class FilterClause;
class SearcherCacheClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace queryparser {

class SearcherCacheParser
{
public:
    SearcherCacheParser(common::ErrorResult *errorResult);
    ~SearcherCacheParser();
private:
    SearcherCacheParser(const SearcherCacheParser &);
    SearcherCacheParser& operator=(const SearcherCacheParser &);
public:
    common::SearcherCacheClause *createSearcherCacheClause();
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
    void setSearcherCacheUse(common::SearcherCacheClause *searcherCacheClause,
                             const std::string &useStr);
    void setSearcherCacheKey(common::SearcherCacheClause *searcherCacheClause,
                             const std::string &keyStr);
    void setSearcherCacheCurTime(
            common::SearcherCacheClause *searcherCacheClause,
            const std::string &curTimeStr);
    void setSearcherCacheDocNum(
            common::SearcherCacheClause *searcherCacheClause,
            const std::string &cacheDocNumStr);
    void setSearcherCacheExpireExpr(
            common::SearcherCacheClause *searcherCacheClause,
            suez::turing::SyntaxExpr *expr);
    void setRefreshAttributes(common::SearcherCacheClause *searcherCacheClause, 
                              const std::string &refreshAttributes);
private:
    bool isOldCacheDocConfig(const autil::StringTokenizer& tokens);
    bool parseOldCacheDocConfig(const autil::StringTokenizer& tokens,         
                                std::vector<uint32_t>& cacheDocNumVec);
    bool parseOldCacheDocNum(const std::string &str, uint32_t &docNum);
    friend class SearcherCacheParserTest;
private:
    common::ErrorResult *_errorResult;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherCacheParser> SearcherCacheParserPtr;

} // namespace queryparser
} // namespace isearch

