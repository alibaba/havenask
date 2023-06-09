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

namespace indexlibv2::config {
class AttributeConfig;
}
namespace indexlibv2::index {

class AttributePatchReader;
class AttributePatchReaderCreator : private autil::NoCopyable
{
public:
    AttributePatchReaderCreator() = default;
    ~AttributePatchReaderCreator() = default;

public:
    static std::shared_ptr<AttributePatchReader> Create(const std::shared_ptr<config::AttributeConfig>& attrConfig);

private:
    static std::shared_ptr<AttributePatchReader>
    CreateMultiValuePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig);

    static std::shared_ptr<AttributePatchReader>
    CreateSingleValuePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
