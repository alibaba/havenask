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

#include "indexlib/document/RawDocument.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::document {

class DeleteValueEncoder
{
public:
    Status Init(const std::shared_ptr<config::ValueConfig>& valueConfig);
    StatusOr<autil::StringView> Encode(const RawDocument& rawDoc, autil::mem_pool::Pool* pool);

private:
    std::vector<std::unique_ptr<index::AttributeConvertor>> _attrConvertors;
    int32_t _valueSize;
};

} // namespace indexlibv2::document
