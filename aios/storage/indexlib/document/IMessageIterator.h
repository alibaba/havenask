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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/StringView.h"

namespace indexlibv2::document {

// an interface that supports a single-pass scan for messages
class IMessageIterator : private autil::NoCopyable
{
public:
    IMessageIterator() {}
    virtual ~IMessageIterator() {}

public:
    virtual bool HasNext() const = 0;
    // return a pair of {message, ingestionTime}
    virtual std::pair<autil::StringView, int64_t> Next() = 0;
    virtual size_t Size() const = 0;
    virtual size_t EstimateMemoryUse() const = 0;
};

} // namespace indexlibv2::document
