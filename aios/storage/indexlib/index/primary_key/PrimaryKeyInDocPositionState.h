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

#include "indexlib/index/inverted_index/InDocPositionState.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace index {

class PrimaryKeyInDocPositionState : public InDocPositionState
{
public:
    PrimaryKeyInDocPositionState() : InDocPositionState(of_none) { SetTermFreq(1); }

    ~PrimaryKeyInDocPositionState() {}

public:
    InDocPositionIterator* CreateInDocIterator() const override { return NULL; }

    void Free() override {}
};

}} // namespace indexlib::index
