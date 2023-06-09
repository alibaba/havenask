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
#include "indexlib/index/inverted_index/format/NormalInDocState.h"

#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, NormalInDocState);

NormalInDocState::NormalInDocState(uint8_t compressMode, const PositionListFormatOption& option)
    : InDocPositionIteratorState(compressMode, option)
    , _owner(nullptr)
{
}

NormalInDocState::NormalInDocState(const NormalInDocState& other)
    : InDocPositionIteratorState(other)
    , _owner(other._owner)
{
}

NormalInDocState::~NormalInDocState() {}

InDocPositionIterator* NormalInDocState::CreateInDocIterator() const
{
    if (_option.HasPositionList()) {
        NormalInDocPositionIterator* iter = new NormalInDocPositionIterator(_option);
        iter->Init(*this);
        return iter;
    } else {
        return nullptr;
    }
}

void NormalInDocState::Clone(NormalInDocState* state) const { InDocPositionIteratorState::Clone(state); }

void NormalInDocState::Free()
{
    if (_owner) {
        _owner->Free(this);
    }
    _posSegDecoder = nullptr;
}
} // namespace indexlib::index
