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
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class IIndexConfig;
}
namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {

class AttributeMemReader
{
public:
    AttributeMemReader() = default;
    virtual ~AttributeMemReader() = default;

public:
    virtual bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen, bool& isNull) = 0;
    virtual size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory) = 0;
    virtual size_t EvaluateCurrentMemUsed() = 0;
};

} // namespace indexlibv2::index
