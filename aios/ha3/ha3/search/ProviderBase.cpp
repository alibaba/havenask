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
#include "ha3/search/ProviderBase.h"

#include <cstddef>

#include "suez/turing/expression/provider/common.h"

#include "ha3/common/AggResultReader.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/rank/GlobalMatchData.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/SearchPluginResource.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MatchValues.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/SimpleMatchData.h"
#include "autil/Log.h"

namespace matchdoc {
template <typename T> class Reference;
}  // namespace matchdoc

using namespace std;
using namespace suez::turing;
using namespace isearch::rank;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, ProviderBase);

ProviderBase::ProviderBase()
    : _searchPluginResource(NULL)
    , _scope(AE_DEFAULT)
{
}

ProviderBase::~ProviderBase() {
}

void ProviderBase::setSearchPluginResource(SearchPluginResource *resource) {
    _searchPluginResource = resource;
}

int32_t ProviderBase::convertRankTraceLevel(const common::Request *request) {
    if (!request) {
        return ISEARCH_TRACE_NONE;
    }
    const auto *configClause = request->getConfigClause();
    if (!configClause) {
        return ISEARCH_TRACE_NONE;
    }
    return common::Tracer::convertLevel(configClause->getRankTrace());
}

const GlobalMatchData *ProviderBase::getGlobalMatchData() const {
    return &_searchPluginResource->globalMatchData;
}

const common::Request *ProviderBase::getRequest() const {
    return _searchPluginResource->request;
}

common::AggResultReader ProviderBase::getAggResultReader() const {
    return common::AggResultReader(_aggResultsPtr);
}

void ProviderBase::setErrorMessage(const std::string &errorMsg) {
    _errorMsg = errorMsg;
}

void ProviderBase::getQueryTermMetaInfo(rank::MetaInfo *metaInfo) const {
    _searchPluginResource->matchDataManager->getQueryTermMetaInfo(metaInfo);
}

common::TimeoutTerminator *ProviderBase::getQueryTerminator() {
    return _searchPluginResource->queryTerminator;
}

#define REQUIRE_MATCHDATA_IMPL(Type, Name, RefName)                     \
matchdoc::Reference<Type>* ProviderBase::require##Name() {        \
    auto subDocDisplayType = SUB_DOC_DISPLAY_NO;                        \
    if (_searchPluginResource->request) {                               \
        auto request = _searchPluginResource->request;                  \
        auto clause = request->getConfigClause();                       \
        if (clause) {                                                   \
            subDocDisplayType = clause->getSubDocDisplayType();         \
        }                                                               \
    }                                                                   \
    auto allocator = _searchPluginResource->matchDocAllocator.get();    \
    auto ret = allocator->findReference<Type>(RefName);                 \
    if (ret) {                                                          \
        return ret;                                                     \
    }                                                                   \
    auto manager = _searchPluginResource->matchDataManager;             \
    if (manager) {                                                      \
        manager->setAttributeExprScope(getAttributeExprScope());        \
        return manager->require##Name(allocator, RefName,               \
                subDocDisplayType, _searchPluginResource->pool);        \
    }                                                                   \
    return nullptr;                                                     \
}

REQUIRE_MATCHDATA_IMPL(MatchData, MatchData, MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(SimpleMatchData, SimpleMatchData, SIMPLE_MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(SimpleMatchData, SubSimpleMatchData, common::SUB_SIMPLE_MATCH_DATA_REF);
REQUIRE_MATCHDATA_IMPL(Ha3MatchValues, MatchValues, MATCH_VALUE_REF);

SearchPluginResource *ProviderBase::getSearchPluginResource() const {
    return _searchPluginResource;
}

} // namespace search
} // namespace isearch
