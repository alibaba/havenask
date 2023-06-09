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

#include "indexlib/index/inverted_index/format/InDocPositionIteratorState.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib::index {
class NormalInDocPositionIterator;

class NormalInDocState : public InDocPositionIteratorState
{
public:
    NormalInDocState(uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE,
                     const PositionListFormatOption& option = PositionListFormatOption());
    NormalInDocState(const NormalInDocState& other);
    ~NormalInDocState();

    InDocPositionIterator* CreateInDocIterator() const override;

    void Free() override;

    void Clone(NormalInDocState* state) const;
    void SetOwner(util::ObjectPool<NormalInDocState>* owner) { _owner = owner; }

protected:
    util::ObjectPool<NormalInDocState>* _owner;

private:
    friend class NormalInDocStateTest;
    friend class PositionListSegmentDecoder;
    friend class InMemPositionListDecoder;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
