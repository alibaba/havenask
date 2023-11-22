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
#include "indexlib/document/document_rewriter/DocumentInfoToAttributeRewriter.h"

#include "indexlib/base/Types.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace {

template <typename T>
static indexlib::Status RewriteSingleField(T value, indexlib::fieldid_t fieldId,
                                           std::shared_ptr<indexlibv2::index::AttributeConvertor> convertor,
                                           indexlibv2::document::NormalDocument* normalDoc)
{
    auto attrDoc = normalDoc->GetAttributeDocument();
    if (!attrDoc) {
        // TODO(xiaohao.yxh) set attr doc if not exist or error?
        return indexlib::Status::Corruption("get attribute document failed");
    }
    bool hasFormatError = false;
    autil::StringView encodedValue =
        convertor->Encode(autil::StringView(std::to_string(value)), normalDoc->GetPool(), hasFormatError);
    if (hasFormatError) {
        return indexlib::Status::Corruption("encoded failed");
    }

    attrDoc->SetField(fieldId, encodedValue);
    return indexlib::Status::OK();
}
} // namespace

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, DocumentInfoToAttributeRewriter);

DocumentInfoToAttributeRewriter::DocumentInfoToAttributeRewriter(
    const std::shared_ptr<index::AttributeConfig>& timestampAttrConfig,
    const std::shared_ptr<index::AttributeConfig>& docInfoAttrConfig)
    : IDocumentRewriter()
    , _timestampFieldId(timestampAttrConfig->GetFieldId())
    , _docInfoFieldId(docInfoAttrConfig->GetFieldId())
{
    _timestampConvertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(timestampAttrConfig));
    _docInfoConvertor.reset(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(docInfoAttrConfig));
}

DocumentInfoToAttributeRewriter::~DocumentInfoToAttributeRewriter() {}

indexlib::Status DocumentInfoToAttributeRewriter::Rewrite(document::IDocumentBatch* batch)
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    while (iter->HasNext()) {
        auto document = iter->Next();
        assert(document);
        if (document->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        auto normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(document.get());
        if (!normalDoc) {
            AUTIL_LOG(ERROR, "cast normal doc failed");
            return indexlib::Status::Corruption("document is not normal document");
        }
        auto docInfo = normalDoc->GetDocInfo();
        int64_t docTs = docInfo.timestamp;
        RETURN_IF_STATUS_ERROR(RewriteSingleField<int64_t>(docTs, _timestampFieldId, _timestampConvertor, normalDoc),
                               "rewrite doc ts [%ld] failed", docTs);

        RETURN_IF_STATUS_ERROR(
            RewriteSingleField<uint64_t>(EncodeDocInfo(docInfo), _docInfoFieldId, _docInfoConvertor, normalDoc),
            "rewrite doc source idx [%u], doc hash id[%u] doc concurrent idx [%u] failed", docInfo.sourceIdx,
            docInfo.hashId, docInfo.concurrentIdx);
    }
    return indexlib::Status::OK();
}

uint64_t DocumentInfoToAttributeRewriter::EncodeDocInfo(const framework::Locator::DocInfo& docInfo)
{
    uint64_t result = 0;
    result |= (uint64_t)DOC_INFO_SERIALIZE_VERSION << 56;
    result |= (uint64_t)docInfo.sourceIdx << 48;
    result |= (uint64_t)docInfo.hashId << 32;
    result |= (uint64_t)docInfo.concurrentIdx;
    return result;
}
std::optional<framework::Locator::DocInfo> DocumentInfoToAttributeRewriter::DecodeDocInfo(uint64_t docInfoValue,
                                                                                          int64_t timestamp)
{
    framework::Locator::DocInfo docInfo;
    auto version = docInfoValue >> 56;
    if (version > DOC_INFO_SERIALIZE_VERSION) {
        return std::nullopt;
    }
    docInfo.sourceIdx = (docInfoValue & (0x00ff000000000000)) >> 48;
    docInfo.hashId = (docInfoValue & (0x0000ffff00000000)) >> 32;
    docInfo.concurrentIdx = docInfoValue & (0x00000000ffffffff);
    docInfo.timestamp = timestamp;
    return docInfo;
}

} // namespace indexlibv2::document
