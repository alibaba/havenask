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

#include "autil/NoCopyable.h"
#include "autil/Span.h"
#include "indexlib/base/Status.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::util {
template <typename T>
class PooledUniquePtr;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::document {
class IIndexFields;
class ExtendDocument;
class DocumentInitParam;

class IIndexFieldsParser : private autil::NoCopyable
{
public:
    virtual ~IIndexFieldsParser() {}

    virtual Status Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                        const std::shared_ptr<document::DocumentInitParam>& initParam) = 0;
    virtual indexlib::util::PooledUniquePtr<IIndexFields>
    Parse(const ExtendDocument& extendDoc, autil::mem_pool::Pool* pool, bool& hasFormatError) const = 0;
    virtual indexlib::util::PooledUniquePtr<IIndexFields> Parse(autil::StringView serializedData,
                                                                autil::mem_pool::Pool* pool) const = 0;
};

} // namespace indexlibv2::document
