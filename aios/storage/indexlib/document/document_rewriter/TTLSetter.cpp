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
#include "indexlib/document/document_rewriter/TTLSetter.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, TTLSetter);

TTLSetter::TTLSetter(const std::shared_ptr<config::AttributeConfig>& ttlAttributeConfig, uint32_t defalutTTLInSeconds)
    : _ttlFieldId(ttlAttributeConfig->GetFieldId())
    , _defalutTTLInSeconds(defalutTTLInSeconds)
    , _ttlFieldName(ttlAttributeConfig->GetAttrName())
{
    _ttlConvertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(ttlAttributeConfig));
}

Status TTLSetter::Rewrite(document::IDocumentBatch* batch)
{
    if (_ttlFieldId == INVALID_FIELDID) {
        assert(false);
        return Status::InternalError("get ttl field id failed");
    }
    int64_t maxTTL = 0;
    for (size_t i = 0; i < batch->GetBatchSize(); ++i) {
        if (batch->IsDropped(i)) {
            continue;
        }
        auto document = (*batch)[i];
        assert(document);
        if (document->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(document.get());
        if (!normalDoc) {
            RETURN_STATUS_ERROR(Status::Corruption, "cast noraml doc failed");
        }
        auto attrDoc = normalDoc->GetAttributeDocument();
        if (!attrDoc) {
            RETURN_STATUS_ERROR(Status::Corruption, "get attribute doc failed");
        }
        const autil::StringView& field = attrDoc->GetField(_ttlFieldId);
        uint32_t ttl = 0;
        if (!field.empty()) {
            index::AttrValueMeta meta = _ttlConvertor->Decode(field);
            assert(sizeof(uint32_t) == meta.data.size());
            ttl = *(uint32_t*)meta.data.data();
        }
        int64_t docTime = autil::TimeUtility::us2sec(normalDoc->GetDocInfo().timestamp);

        if (ttl == 0 && _ttlFieldName == DOC_TIME_TO_LIVE_IN_SECONDS) {
            ttl = std::min((uint64_t)std::numeric_limits<uint32_t>::max(), (uint64_t)_defalutTTLInSeconds + docTime);
            bool hasFormatError = false;
            autil::StringView encodedValue =
                _ttlConvertor->Encode(autil::StringView(std::to_string(ttl)), normalDoc->GetPool(), hasFormatError);
            if (hasFormatError) {
                return Status::Corruption("encoded ttl value [%d] failed", ttl);
            }
            attrDoc->SetField(_ttlFieldId, encodedValue);
            AUTIL_INTERVAL_LOG2(30, WARN, "get ttl value is invalid 0 set to [%d] for pk [%s]", ttl,
                                normalDoc->GetPrimaryKey().c_str());
        }

        normalDoc->SetTTL(ttl);
        maxTTL = std::max(maxTTL, (int64_t)ttl);
    }
    batch->SetMaxTTL(maxTTL);
    return Status::OK();
}

TTLSetter::~TTLSetter() {}

} // namespace indexlibv2::document
