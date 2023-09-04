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

#include <map>
#include <string>
#include <unordered_set>

#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/turing/common/ModelConfig.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

namespace autil {
class TimeoutTerminator;

namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace build_service {
namespace analyzer {
class AnalyzerFactory;
} // namespace analyzer
} // namespace build_service
namespace isearch {
namespace common {
class QueryInfo;
} // namespace common
namespace search {
class LayerMeta;
} // namespace search
} // namespace isearch
namespace sql {
class IndexInfo;
class UdfToQueryManager;
} // namespace sql

namespace suez {
namespace turing {
class IndexInfos;
} // namespace turing
} // namespace suez

namespace sql {
class Ha3ScanConditionVisitorParam {
public:
    Ha3ScanConditionVisitorParam()
        : analyzerFactory(nullptr)
        , pool(nullptr)
        , queryInfo(nullptr)
        , indexInfos(nullptr)
        , timeoutTerminator(nullptr)
        , layerMeta(nullptr)
        , modelConfigMap(nullptr)
        , udfToQueryManager(nullptr)
        , forbidIndexs(nullptr) {}
    suez::turing::AttributeExpressionCreatorPtr attrExprCreator;
    isearch::search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    const build_service::analyzer::AnalyzerFactory *analyzerFactory;
    const std::map<std::string, IndexInfo> *indexInfo;
    autil::mem_pool::Pool *pool;
    const isearch::common::QueryInfo *queryInfo;
    const suez::turing::IndexInfos *indexInfos;
    std::string mainTableName;
    autil::TimeoutTerminator *timeoutTerminator;
    isearch::search::LayerMeta *layerMeta;
    const isearch::turing::ModelConfigMap *modelConfigMap;
    UdfToQueryManager *udfToQueryManager;
    const std::unordered_set<std::string> *forbidIndexs;
};

} // namespace sql
