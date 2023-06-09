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

#include "autil/StringView.h"
#include "indexlib/base/Status.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {

class ValueReader
{
public:
    virtual ~ValueReader() = default;

public:
    virtual Status Read(uint64_t offset, autil::mem_pool::Pool* pool, autil::StringView& data) = 0;
};

} // namespace indexlibv2::index
