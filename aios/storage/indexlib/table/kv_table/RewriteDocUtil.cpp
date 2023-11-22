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
#include "indexlib/table/kv_table/RewriteDocUtil.h"

#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, RewriteDocUtil);

Status RewriteDocUtil::RewriteMergeValue(const autil::StringView& currentValue, const autil::StringView& newValue,
                                         std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& formatter,
                                         indexlib::util::MemBufferPtr& memBuffer, document::KVDocument* doc)
{
    if (formatter) {
        indexlibv2::index::PackAttributeFormatter::PackAttributeFields updateFields;
        auto ret = formatter->DecodePatchValues((uint8_t*)newValue.data(), newValue.size(), updateFields);
        if (!ret) {
            auto status = Status::InternalError("decode patch values failed key = [%s], keyHash = [%ld]",
                                                doc->GetPkFieldValue(), doc->GetPKeyHash());
            AUTIL_LOG(WARN, "%s", status.ToString().c_str());
            return status;
        }
        auto mergeValue = formatter->MergeAndFormatUpdateFields(currentValue.data(), updateFields,
                                                                /*hasHashKeyInAttrFields*/ true, *memBuffer);
        doc->SetValue(mergeValue); // TODO(xinfei.sxf) use multi memBuffer to reduce the cost of copy
        doc->SetDocOperateType(ADD_DOC);
    } else {
        auto status = Status::InternalError("formatter init failed key = [%s], pkeyHash = [%ld], skeyHash = [%ld]",
                                            doc->GetPkFieldValue(), doc->GetPKeyHash(), doc->GetSKeyHash());
        AUTIL_LOG(WARN, "%s", status.ToString().c_str());
        return status;
    }
    return Status::OK();
}

void RewriteDocUtil::RewriteValue(std::shared_ptr<indexlibv2::index::AttributeConvertor>& attrConvertor,
                                  autil::StringView& value)
{
    auto meta = attrConvertor->Decode(value);
    value = meta.data;
    size_t encodeCountLen = 0;
    autil::MultiValueFormatter::decodeCount(value.data(), encodeCountLen);
    value = autil::StringView(value.data() + encodeCountLen, value.size() - encodeCountLen);
}

} // namespace indexlibv2::table