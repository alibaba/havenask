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
#include "ha3/search/DefaultSearcherCacheStrategy.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/func_expression/FunctionProvider.h"
#include "ha3/isearch.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/Filter.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheStrategy.h"
#include "autil/Log.h"

namespace matchdoc {
class MatchDocAllocator;
}  // namespace matchdoc

using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, DefaultSearcherCacheStrategy);

const uint32_t DefaultSearcherCacheStrategy::DEFAULT_EXPIRE_TIME_IN_SECONDS = 30;

class AllocatorScope {
public:
    AllocatorScope(func_expression::FunctionProvider *provider,
                   common::Ha3MatchDocAllocator *allocator)
        : _provider(provider)
    {
        if (_provider) {
            _oldAllocator = _provider->setAllocator(allocator);
        }
    }
    ~AllocatorScope() {
        if (_provider) {
            _provider->setAllocator(_oldAllocator);
        }
    }
private:
    func_expression::FunctionProvider *_provider;
    matchdoc::MatchDocAllocator *_oldAllocator;
};

DefaultSearcherCacheStrategy::DefaultSearcherCacheStrategy(mem_pool::Pool *pool,
        bool hasSubSchema,
        SearchCommonResource &resource,
        SearchPartitionResource &searchPartitionResource)
    : _pool(pool)
    , _partitionReaderSnapshot(searchPartitionResource.partitionReaderSnapshot)
    , _cavaAllocator(resource.cavaAllocator)
    , _functionProvider(&searchPartitionResource.functionProvider)
    , _functionCreator(resource.functionCreator)
    , _hasSubSchema(hasSubSchema)
    , _mainTable(searchPartitionResource.mainTable)
    , _tableInfoPtr(resource.tableInfoPtr)
    , _cavaPluginManager(resource.cavaPluginManager)
{
}

DefaultSearcherCacheStrategy::DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool)
    : _pool(pool)
{
}

DefaultSearcherCacheStrategy::~DefaultSearcherCacheStrategy() {
    POOL_DELETE_CLASS(_filter);
    POOL_DELETE_CLASS(_attributeExpressionCreator);
}

bool DefaultSearcherCacheStrategy::init(const Request *request,
                                        MultiErrorResult* errorResult)
{
    _matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(
                    _pool, needSubDoc(request)));
    AllocatorScope scope(_functionProvider, _matchDocAllocatorPtr.get());
    if (!initAttributeExpressionCreator(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "init AttributeExpressionCreator failed");
        AUTIL_LOG(WARN, "init AttributeExpressionCreator failed");
        return false;
    }

    if (!createExpireTimeExpr(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "init ExpireTimeExpr failed");
        AUTIL_LOG(WARN, "init ExpireTimeExpr failed");
        return false;
    }

    if (!createFilter(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "create Filter failed");
        AUTIL_LOG(WARN, "create Filter failed");
        return false;
    }

    return true;
}

bool DefaultSearcherCacheStrategy::initAttributeExpressionCreator(
        const Request *request)
{
    // for test
    if (_attributeExpressionCreator != NULL) {
        return true;
    }
    _attributeExpressionCreator = POOL_NEW_CLASS(_pool,
            AttributeExpressionCreator,
            _pool,
            _matchDocAllocatorPtr.get(),
            _mainTable,
            _partitionReaderSnapshot,
            _tableInfoPtr,
            (request->getVirtualAttributeClause()
             ? request->getVirtualAttributeClause()->getVirtualAttributes()
             : suez::turing::VirtualAttributes()),
            _functionCreator,
            _cavaPluginManager,
            _functionProvider);
    return true;
}

bool DefaultSearcherCacheStrategy::genKey(const Request *request,
        uint64_t &key)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    if (!cacheClause) {
        return false;
    }

    if (!cacheClause->getUse()) {
        return false;
    }

    key = cacheClause->getKey();
    return true;
}

bool DefaultSearcherCacheStrategy::beforePut(uint32_t curTime,
        CacheResult *cacheResult)
{
    if (curTime == SearcherCacheClause::INVALID_TIME) {
        curTime = TimeUtility::currentTimeInSeconds();
    }
    uint32_t defaultExpireTime = curTime + DEFAULT_EXPIRE_TIME_IN_SECONDS;

    if (!calcAndfillExpireTime(cacheResult, defaultExpireTime)) {
        return false;
    }

    cacheResult->getHeader()->time = curTime;
    return true;
}

bool DefaultSearcherCacheStrategy::calcAndfillExpireTime(
        CacheResult *cacheResult, uint32_t defaultExpireTime)
{
    if (_expireTimeExpr) {
        common::Result *result = cacheResult->getResult();
        assert(result);
        MatchDocs *matchDocs = result->getMatchDocs();
        assert(matchDocs);

        uint32_t expireTime = (uint32_t)-1;
        if (!calcExpireTime(_expireTimeExpr, matchDocs, expireTime)) {
            return false;
        }
        if (expireTime == (uint32_t)-1) {
            expireTime = defaultExpireTime;
        }
        cacheResult->getHeader()->expireTime = expireTime;
    }
    return true;
}

uint32_t DefaultSearcherCacheStrategy::getCacheDocNum(const Request *request,
        uint32_t requireTopK)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);
    const vector<uint32_t> &cacheDocNumVec = cacheClause->getCacheDocNumVec();
    uint32_t size = cacheDocNumVec.size();
    assert(size > 0);
    for (size_t i = 0; i < size; ++i) {
        if (cacheDocNumVec[i] >= requireTopK) {
            return cacheDocNumVec[i];
        }
    }
    return *cacheDocNumVec.rbegin();
}

bool DefaultSearcherCacheStrategy::calcExpireTime(
        AttributeExpression *expireExpr,
        MatchDocs *matchDocs, uint32_t &expireTime)
{
    expireTime = (uint32_t)-1;
    auto ref = dynamic_cast<matchdoc::Reference<uint32_t> *>(
            expireExpr->getReferenceBase());
    if (ref == NULL) {
        AUTIL_LOG(WARN, "invalid expire time expr reference type:[%d]",
                (int)expireExpr->getType());
        return false;
    }

    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto orignalMatchDoc = matchDocs->getMatchDoc(i);
        if (matchdoc::INVALID_MATCHDOC == orignalMatchDoc) {
            return false;
        }
        auto tmpMatchDoc = allocateTmpMatchDoc(matchDocs, i);
        if (!expireExpr->evaluate(tmpMatchDoc)) {
            AUTIL_LOG(WARN, "calcluate expire time for doc[%d] failed" ,
                    tmpMatchDoc.getDocId());
            _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
            return false;
        }
        uint32_t tmpVal = 0;
        tmpVal = ref->get(tmpMatchDoc);
        expireTime = expireTime > tmpVal ? tmpVal : expireTime;
        _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
    }
    return true;
}

bool DefaultSearcherCacheStrategy::createExpireTimeExpr(
        const Request *request)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);
    SyntaxExpr *expireSyntaxExpr = cacheClause->getExpireExpr();
    if (expireSyntaxExpr) {
        assert(!expireSyntaxExpr->isSubExpression());
        assert(_attributeExpressionCreator != NULL);
        AttributeExpression *expireExpr =
            _attributeExpressionCreator->createAttributeExpression(
                    expireSyntaxExpr);
        if (!expireExpr || !expireExpr->allocate(_matchDocAllocatorPtr.get())) {
            AUTIL_LOG(WARN, "create expire time expression failed");
            return false;
        }
        _expireTimeExpr = expireExpr;
    }

    return true;
}

bool DefaultSearcherCacheStrategy::createFilter(const Request *request) {
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);

    FilterClause *filterClause = cacheClause->getFilterClause();
    if (filterClause) {
        assert(!filterClause->getRootSyntaxExpr()->isSubExpression());
        _filter = Filter::createFilter(filterClause->getRootSyntaxExpr(),
                _attributeExpressionCreator, _pool);
        if (_filter == NULL) {
            return false;
        }
    }

    return true;
}

uint32_t DefaultSearcherCacheStrategy::filterCacheResult(
        CacheResult *cacheResult)
{
    common::Result *result = cacheResult->getResult();
    assert(result);
    MatchDocs *matchDocs = result->getMatchDocs();
    assert(matchDocs);
    uint32_t delCount = filterCacheResult(matchDocs);
    if (delCount > 0) {
        releaseDeletedMatchDocs(matchDocs);
    }
    return delCount;
}

uint32_t DefaultSearcherCacheStrategy::filterCacheResult(MatchDocs *matchDocs)
{
    uint32_t delCount = 0;
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto &matchDoc = matchDocs->getMatchDoc(i);
        if (matchDoc.isDeleted()) {
            delCount++;
            continue;
        }
        if (!_filter) {
            continue;
        }
        auto tmpMatchDoc = allocateTmpMatchDoc(matchDocs, i);
        if (!_filter->pass(tmpMatchDoc)) {
            matchDoc.setDeleted();
            delCount++;
        }
        _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
    }
    return delCount;
}

matchdoc::MatchDoc DefaultSearcherCacheStrategy::allocateTmpMatchDoc(
        MatchDocs *matchDocs, uint32_t pos)
{
    auto matchDoc = matchDocs->getMatchDoc(pos);
    auto ret = _matchDocAllocatorPtr->allocate(matchDoc.getDocId());
    if (!_matchDocAllocatorPtr->hasSubDocAllocator()) {
        return ret;
    }
    const auto &allocator = matchDocs->getMatchDocAllocator();
    auto accessor = allocator->getSubDocAccessor();
    vector<int32_t> subDocids;
    accessor->getSubDocIds(matchDoc, subDocids);
    for (const auto &subDocId : subDocids) {
        _matchDocAllocatorPtr->allocateSub(ret, subDocId);
    }
    return ret;
}

void DefaultSearcherCacheStrategy::releaseDeletedMatchDocs(
        MatchDocs *matchDocs)
{
    const auto &allocator = matchDocs->getMatchDocAllocator();
    assert(allocator);
    vector<matchdoc::MatchDoc> matchDocsVect;
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto matchDoc = matchDocs->getMatchDoc(i);
        if (matchDoc.isDeleted()) {
            allocator->deallocate(matchDoc);
        } else {
            matchDocsVect.push_back(matchDoc);
        }
    }
    if (matchDocsVect.size() != matchDocs->size()) {
        auto &matchDocsVectRef = matchDocs->getMatchDocsVect();
        matchDocsVectRef = matchDocsVect;
    }
}

bool DefaultSearcherCacheStrategy::needSubDoc(const Request *request) const {
    ConfigClause *configClause = request->getConfigClause();
    if (configClause == NULL) {
        return false;
    }
    return configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO && _hasSubSchema;
}

} // namespace search
} // namespace isearch
