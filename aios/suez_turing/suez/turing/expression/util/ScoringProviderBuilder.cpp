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
#include "suez/turing/expression/util/ScoringProviderBuilder.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "matchdoc/VectorDocStorage.h"
#include "suez/turing/expression/provider/ProviderCreator.h"
#include "turing_ops_util/variant/Tracer.h"

namespace cava {
class CavaJitModulesWrapper;
} // namespace cava

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace suez {
namespace turing {
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace tensorflow;

#define REQUIRE_TRUE(expr)                                                                                             \
    if (unlikely(!(expr))) {                                                                                           \
        AUTIL_LOG(ERROR, "`" #expr "` check failed");                                                                  \
        return false;                                                                                                  \
    }

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, ScoringProviderBuilder);

ScoringProviderBuilder::ScoringProviderBuilder()
    : _pool(nullptr)
    , _tracer(nullptr)
    , _rankTraceLevel(ISEARCH_TRACE_NONE)
    , _partitionReaderSnapshot(nullptr)
    , _cavaAllocator(nullptr)
    , _virtualAttrs(nullptr)
    , _kvPairs(nullptr) {}

bool ScoringProviderBuilder::init(const string &mainTable,
                                  const TableInfoPtr &tableInfo,
                                  FunctionInterfaceCreator *functionCreator,
                                  const CavaPluginManager *cavaPluginManager,
                                  autil::mem_pool::Pool *pool,
                                  Tracer *tracer,
                                  int32_t rankTraceLevel,
                                  indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                                  SuezCavaAllocator *cavaAllocator,
                                  cava::CavaJitModulesWrapper *cavaJitModulesWrapper,
                                  VirtualAttributes *virtualAttrs,
                                  const KeyValueMap *kvPairs,
                                  const matchdoc::MatchDocAllocatorPtr &allocator) {
    _mainTable = mainTable;
    _tableInfo = tableInfo;
    _functionCreator = functionCreator;
    _cavaPluginManager = cavaPluginManager;
    _pool = pool;
    _tracer = tracer;
    _rankTraceLevel = rankTraceLevel;
    _partitionReaderSnapshot = partitionReaderSnapshot;
    _cavaAllocator = cavaAllocator;
    _cavaJitModulesWrapper = cavaJitModulesWrapper;
    _virtualAttrs = virtualAttrs;
    _kvPairs = kvPairs;
    _allocator = allocator;
    REQUIRE_TRUE(checkBasicResourceRequirement());
    return true;
}

bool ScoringProviderBuilder::checkBasicResourceRequirement() {
    REQUIRE_TRUE(!_mainTable.empty());
    REQUIRE_TRUE(_tableInfo);
    REQUIRE_TRUE(_partitionReaderSnapshot);
    REQUIRE_TRUE(_cavaPluginManager);
    REQUIRE_TRUE(_functionCreator);
    REQUIRE_TRUE(_pool);
    REQUIRE_TRUE(_cavaAllocator);
    REQUIRE_TRUE(_virtualAttrs);
    REQUIRE_TRUE(_kvPairs);
    REQUIRE_TRUE(_allocator);
    return true;
}

FunctionProvider *ScoringProviderBuilder::getFunctionProvider() {
    if (!_functionProvider) {
        if (!checkBasicResourceRequirement()) {
            return nullptr;
        }
        _functionProvider.reset(
            new FunctionProvider(_allocator.get(), _pool, _cavaAllocator, _tracer, _partitionReaderSnapshot, _kvPairs));
    }
    return _functionProvider.get();
}

bool ScoringProviderBuilder::genAttributeExpressionCreator() {
    if (_attributeExpressionCreator) {
        return true;
    }
    REQUIRE_TRUE(checkBasicResourceRequirement());
    auto *functionProvider = getFunctionProvider();
    REQUIRE_TRUE(functionProvider);

    _attributeExpressionCreator.reset(new AttributeExpressionCreator(_pool,
                                                                     _allocator.get(),
                                                                     _mainTable,
                                                                     _partitionReaderSnapshot,
                                                                     _tableInfo,
                                                                     *_virtualAttrs,
                                                                     _functionCreator,
                                                                     _cavaPluginManager,
                                                                     functionProvider));
    return true;
}

bool ScoringProviderBuilder::genProvider() {
    if (_scoringProvider && _scorerProvider) {
        return true;
    }
    REQUIRE_TRUE(checkBasicResourceRequirement());
    REQUIRE_TRUE(genAttributeExpressionCreator());

    ProviderParam param(_allocator.get(),
                        _pool,
                        _cavaAllocator,
                        _tracer,
                        _partitionReaderSnapshot,
                        _kvPairs,
                        _rankTraceLevel,
                        _attributeExpressionCreator.get());
    ProviderCreator providerCreator(param);
    _scoringProvider = providerCreator.createScoringProvider();
    _scorerProvider.reset(new ScorerProvider(_scoringProvider.get(), _cavaJitModulesWrapper));
    return true;
}

// for test
bool ScoringProviderTestBuilder::checkBasicResourceRequirement() {
    if (_needCheckIndex) {
        REQUIRE_TRUE(!_mainTable.empty());
        REQUIRE_TRUE(_tableInfo);
        REQUIRE_TRUE(_partitionReaderSnapshot);
    }
    if (_needCheckCavaPluginManager) {
        REQUIRE_TRUE(_cavaPluginManager);
    }
    REQUIRE_TRUE(_functionCreator);
    REQUIRE_TRUE(_pool);
    REQUIRE_TRUE(_cavaAllocator);
    REQUIRE_TRUE(_virtualAttrs);
    REQUIRE_TRUE(_kvPairs);
    REQUIRE_TRUE(_allocator);
    return true;
}

} // namespace turing
} // namespace suez
