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

#include "indexlib/base/Status.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

class ValueExtractorUtil
{
public:
    static Status InitValueExtractor(const indexlibv2::config::KVIndexConfig& kvIndexConfig,
                                     std::shared_ptr<AttributeConvertor>& convertor,
                                     std::shared_ptr<PlainFormatEncoder>& encoder);
};

} // namespace indexlibv2::index
