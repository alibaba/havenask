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
#include "suez/turing/expression/provider/MatchInfoReader.h"

#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/provider/MetaInfo.h"

using namespace std;
using namespace matchdoc;

namespace suez {
namespace turing {

MatchInfoReader::MatchInfoReader()
    : _metaInfo(), _simpleMatchDataRef(nullptr), _matchDataRef(nullptr), _matchValuesRef(nullptr) {}

MatchInfoReader::~MatchInfoReader() { reset(); }

void MatchInfoReader::reset() {
    _metaInfo.reset();
    _simpleMatchDataRef = nullptr;
    _matchDataRef = nullptr;
    _matchValuesRef = nullptr;
}

void MatchInfoReader::init(std::shared_ptr<MetaInfo> metaInfo, matchdoc::MatchDocAllocator *allocator) {
    reset();
    _metaInfo = std::move(metaInfo);
    _simpleMatchDataRef = allocator->findReference<SimpleMatchData>(SIMPLE_MATCH_DATA_REF);
    _matchDataRef = allocator->findReference<MatchData>(MATCH_DATA_REF);
    _matchValuesRef = allocator->findReference<MatchValues>(MATCH_VALUE_REF);
}
} // namespace turing
} // namespace suez
