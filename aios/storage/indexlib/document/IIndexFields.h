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

#include "autil/DataBuffer.h"
#include "autil/NoCopyable.h"
#include "autil/StringView.h"

namespace indexlibv2::document {

class IIndexFields : private autil::NoCopyable
{
public:
    virtual ~IIndexFields() {}

    virtual void serialize(autil::DataBuffer& dataBuffer) const = 0;
    // caller should make sure inner data buffer of dataBuffer is alway valid before IndexFields deconstruct
    virtual void deserialize(autil::DataBuffer& dataBuffer) = 0;

    virtual autil::StringView GetIndexType() const = 0;
    virtual size_t EstimateMemory() const = 0;
};

} // namespace indexlibv2::document
