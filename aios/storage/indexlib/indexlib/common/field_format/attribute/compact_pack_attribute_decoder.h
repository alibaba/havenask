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

#include "indexlib/common/field_format/attribute/attribute_convertor.h"

namespace indexlib { namespace common {

class CompactPackAttributeDecoder : public AttributeConvertor
{
public:
    explicit CompactPackAttributeDecoder(AttributeConvertor* impl);
    ~CompactPackAttributeDecoder();

public:
    AttrValueMeta Decode(const autil::StringView& str) override;

    std::string EncodeNullValue() override;
    autil::StringView EncodeFromAttrValueMeta(const AttrValueMeta& attrValueMeta,
                                              autil::mem_pool::Pool* memPool) override;
    autil::StringView EncodeFromRawIndexValue(const autil::StringView& rawValue,
                                              autil::mem_pool::Pool* memPool) override;

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& strResult, char* outBuffer, EncodeStatus& status) override;

private:
    std::unique_ptr<AttributeConvertor> mImpl;
};

}} // namespace indexlib::common
