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
#include "indexlib/index/common/field_format/attribute/CompactPackAttributeDecoder.h"

#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, CompactPackAttributeDecoder);

CompactPackAttributeDecoder::CompactPackAttributeDecoder(AttributeConvertor* impl)
    : AttributeConvertor(false, "")
    , _impl(impl)
{
}

AttrValueMeta CompactPackAttributeDecoder::Decode(const autil::StringView& str)
{
    auto meta = _impl->Decode(str);
    auto tempData = meta.data;
    size_t headerSize = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*tempData.data());
    meta.data = autil::StringView(tempData.data() + headerSize, tempData.size() - headerSize);
    return meta;
}

std::pair<Status, std::string> CompactPackAttributeDecoder::EncodeNullValue()
{
    assert(false);
    return std::make_pair(Status::Unimplement(), std::string(""));
}

autil::StringView CompactPackAttributeDecoder::EncodeFromAttrValueMeta(const AttrValueMeta& attrValueMeta,
                                                                       autil::mem_pool::Pool* memPool)
{
    assert(false);
    return autil::StringView::empty_instance();
}

autil::StringView CompactPackAttributeDecoder::EncodeFromRawIndexValue(const autil::StringView& rawValue,
                                                                       autil::mem_pool::Pool* memPool)
{
    assert(false);
    return autil::StringView::empty_instance();
}

autil::StringView CompactPackAttributeDecoder::InnerEncode(const autil::StringView& attrData,
                                                           autil::mem_pool::Pool* memPool, std::string& strResult,
                                                           char* outBuffer, EncodeStatus& status)
{
    assert(false);
    return autil::StringView::empty_instance();
}
} // namespace indexlibv2::index
