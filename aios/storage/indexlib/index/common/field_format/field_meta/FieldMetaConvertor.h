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
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {

struct FieldValueMeta {
    std::string fieldValue;
    size_t tokenCount = 0;
};

class FieldMetaConvertor : private autil::NoCopyable
{
public:
    FieldMetaConvertor() = default;
    ~FieldMetaConvertor() = default;

public:
    bool Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);
    // text field 是分词后的 token 数， 其他类型的填 0
    autil::StringView Encode(const autil::StringView& fieldValue, autil::mem_pool::Pool* memPool, size_t tokenCount);
    std::pair<bool, FieldValueMeta> Decode(const autil::StringView& str);

private:
    std::shared_ptr<indexlibv2::index::AttributeConvertor> _attributeConvertor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
