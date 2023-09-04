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
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"

#include <cstddef>
#include <map>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/partition/join_cache/join_docid_reader_creator.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/partition_version.h"
#include "matchdoc/VectorDocStorage.h"
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"

namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

using namespace std;
using namespace autil;
using namespace matchdoc;

using namespace indexlib::partition;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, JoinDocIdConverterCreator);

JoinDocIdConverterCreator::JoinDocIdConverterCreator(autil::mem_pool::Pool *pool,
                                                     matchdoc::MatchDocAllocator *matchDocAllocator)
    : _matchDocAllocator(matchDocAllocator), _pool(pool), _hasStrongJoinConverter(false) {}

JoinDocIdConverterCreator::~JoinDocIdConverterCreator() {
    map<string, JoinDocIdConverterBase *>::iterator iter = _converterMap.begin();
    for (; iter != _converterMap.end(); iter++) {
        POOL_DELETE_CLASS(iter->second);
        iter->second = NULL;
    }
    _converterMap.clear();
}

JoinDocIdConverterBase *JoinDocIdConverterCreator::createJoinDocIdConverter(const JoinInfo &joinInfo) {
    const string &joinFieldName = joinInfo.GetJoinFieldName();
    auto iter = _converterMap.find(joinFieldName);
    if (iter != _converterMap.end()) {
        return iter->second;
    }

    auto joinDocidReader = JoinDocidReaderCreator::Create(joinInfo, _pool);
    if (!joinDocidReader) {
        return NULL;
    }
    JoinDocIdConverterBase *converter = POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, joinFieldName, joinDocidReader);
    if (!converter->init(_matchDocAllocator, joinInfo.IsSubJoin())) {
        POOL_DELETE_CLASS(converter);
        return NULL;
    }
    if (converter) {
        bool strongJoin = joinInfo.IsStrongJoin();
        _hasStrongJoinConverter = _hasStrongJoinConverter || strongJoin;
        converter->setStrongJoin(strongJoin);
        _converterMap[joinFieldName] = converter;
    }
    return converter;
}

} // namespace turing
} // namespace suez
