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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/search/SearcherCacheStrategy.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace common {
class MatchDocs;
class MultiErrorResult;
class Request;
}  // namespace common
namespace func_expression {
class FunctionProvider;
}  // namespace func_expression
namespace search {
class CacheResult;
class Filter;
class SearchCommonResource;
class SearchPartitionResource;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionCreator;
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class DefaultSearcherCacheStrategy : public SearcherCacheStrategy
{
public:
    DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool,
                                 bool hasSubSchema,
                                 SearchCommonResource &resource,
                                 SearchPartitionResource &searchPartitionResource);
    ~DefaultSearcherCacheStrategy();
private:
    DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool);
    DefaultSearcherCacheStrategy(const DefaultSearcherCacheStrategy &);
    DefaultSearcherCacheStrategy& operator=(const DefaultSearcherCacheStrategy &);
public:
    virtual bool init(const common::Request *request, common::MultiErrorResult* errorResult);
    virtual bool genKey(const common::Request *request, uint64_t &key);
    virtual bool beforePut(uint32_t curTime, CacheResult *cacheResult);
    virtual uint32_t filterCacheResult(CacheResult *cacheResult);
    virtual uint32_t getCacheDocNum(const common::Request *request,
                                    uint32_t docFound);
private:
    bool initAttributeExpressionCreator(const common::Request *request);
    bool createExpireTimeExpr(const common::Request *request);
    bool createFilter(const common::Request *request);
    bool calcAndfillExpireTime(CacheResult *cacheResult, uint32_t defaultExpireTime);
    bool calcExpireTime(suez::turing::AttributeExpression *expireExpr,
                        common::MatchDocs *matchDocs,
                        uint32_t &expireTime);
    uint32_t filterCacheResult(common::MatchDocs *matchDocs);
    void releaseDeletedMatchDocs(common::MatchDocs *matchDocs);

    suez::turing::AttributeExpression* getExpireTimeExpr() const { return _expireTimeExpr;}
    Filter* getFilter() const { return _filter;}

    bool needSubDoc(const common::Request *request) const;

    matchdoc::MatchDoc allocateTmpMatchDoc(
            common::MatchDocs *matchDocs, uint32_t pos);

    friend class DefaultSearcherCacheStrategyTest;
private:
    autil::mem_pool::Pool *_pool = nullptr;
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator = nullptr;
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    suez::turing::AttributeExpression* _expireTimeExpr = nullptr;
    Filter* _filter = nullptr;
    suez::turing::SuezCavaAllocator *_cavaAllocator = nullptr;
    func_expression::FunctionProvider *_functionProvider = nullptr;
    const suez::turing::FunctionInterfaceCreator *_functionCreator = nullptr;
    common::Ha3MatchDocAllocatorPtr _matchDocAllocatorPtr;
    bool _hasSubSchema;
    // for new AttributeExpressionCreator
    std::string _mainTable;
    suez::turing::TableInfoPtr _tableInfoPtr;
    const suez::turing::CavaPluginManager *_cavaPluginManager;
private:
    static const uint32_t DEFAULT_EXPIRE_TIME_IN_SECONDS;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultSearcherCacheStrategy> DefaultSearcherCacheStrategyPtr;

} // namespace search
} // namespace isearch
