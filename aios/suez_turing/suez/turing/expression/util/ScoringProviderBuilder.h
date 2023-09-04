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

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "cava/codegen/CavaJitModule.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/cava/impl/ScorerProvider.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/provider/ScoringProvider.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

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
namespace tensorflow {
class SessionResource;
} // namespace tensorflow

namespace suez {
namespace turing {
class FunctionInterfaceCreator;
class SuezCavaAllocator;
class Tracer;

class ScoringProviderBuilder {
public:
    ScoringProviderBuilder();
    virtual ~ScoringProviderBuilder() {}
    ScoringProviderBuilder(const ScoringProviderBuilder &) = delete;
    ScoringProviderBuilder &operator=(const ScoringProviderBuilder &) = delete;

public:
    bool init(const std::string &mainTable,
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
              const matchdoc::MatchDocAllocatorPtr &allocator);

public:
    virtual FunctionProvider *getFunctionProvider();
    bool genAttributeExpressionCreator();
    bool genProvider();

    AttributeExpressionCreator *getAttributeExpressionCreator() { return _attributeExpressionCreator.get(); }

    ScoringProvider *getScoringProvider() { return _scoringProvider.get(); }

    ScorerProvider *getScorerProvider() { return _scorerProvider.get(); }

private:
    virtual bool checkBasicResourceRequirement();

protected:
    std::string _mainTable;
    TableInfoPtr _tableInfo;
    FunctionInterfaceCreator *_functionCreator = nullptr;
    const CavaPluginManager *_cavaPluginManager = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    Tracer *_tracer = nullptr;
    int32_t _rankTraceLevel;
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    SuezCavaAllocator *_cavaAllocator = nullptr;
    cava::CavaJitModulesWrapper *_cavaJitModulesWrapper = nullptr;
    VirtualAttributes *_virtualAttrs = nullptr;
    const KeyValueMap *_kvPairs = nullptr;
    matchdoc::MatchDocAllocatorPtr _allocator;

    FunctionProviderPtr _functionProvider;
    AttributeExpressionCreatorPtr _attributeExpressionCreator;
    ScoringProviderPtr _scoringProvider;
    ScorerProviderPtr _scorerProvider;

protected:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScoringProviderBuilder> ScoringProviderBuilderPtr;

// for test
class ScoringProviderTestBuilder : public ScoringProviderBuilder {
public:
    void setNeedCheckCavaPluginManager(bool flag) { _needCheckCavaPluginManager = flag; }

private:
    bool checkBasicResourceRequirement() override;

private:
    bool _needCheckIndex = true;
    bool _needCheckCavaPluginManager = true;
};

} // namespace turing
} // namespace suez
