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

#include "autil/LongHashValue.h"

namespace indexlib { namespace index {

class PrimaryKeyHashConvertor
{
public:
    PrimaryKeyHashConvertor() {}
    ~PrimaryKeyHashConvertor() {}

public:
    static void ToUInt128(uint64_t in, autil::uint128_t& out) { out.value[1] = in; }

    static void ToUInt64(const autil::uint128_t& in, uint64_t& out) { out = in.value[1]; }
};

}} // namespace indexlib::index
