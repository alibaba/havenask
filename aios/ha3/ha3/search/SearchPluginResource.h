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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/rank/GlobalMatchData.h"
#include "indexlib/misc/common.h"

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
class DataProvider;
class Request;
}  // namespace common
namespace search {
class MatchDataManager;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpressionCreatorBase;
class FieldInfos;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class SearchPluginResource
{
public:
    autil::mem_pool::Pool *pool = nullptr;
    suez::turing::SuezCavaAllocator *cavaAllocator = nullptr;
    const common::Request *request = nullptr;
    rank::GlobalMatchData globalMatchData;
    suez::turing::AttributeExpressionCreatorBase *attrExprCreator = nullptr;
    common::DataProvider *dataProvider = nullptr;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator;
    const suez::turing::FieldInfos *fieldInfos = nullptr;
    MatchDataManager *matchDataManager = nullptr;
    common::TimeoutTerminator *queryTerminator = nullptr;
    common::Tracer *requestTracer = nullptr;
    const KeyValueMap *kvpairs = nullptr;
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot = nullptr;
};

typedef std::shared_ptr<SearchPluginResource> SearchPluginResourcePtr;

} // namespace search
} // namespace isearch

