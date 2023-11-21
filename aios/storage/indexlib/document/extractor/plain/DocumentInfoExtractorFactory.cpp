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
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"

#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/plain/AttrFieldInfoExtractor.h"
#include "indexlib/document/extractor/plain/FieldMetaFieldInfoExtractor.h"
#include "indexlib/document/extractor/plain/InvertedIndexDocInfoExtractor.h"
#include "indexlib/document/extractor/plain/PackAttrFieldInfoExtractor.h"
#include "indexlib/document/extractor/plain/PrimaryKeyInfoExtractor.h"
#include "indexlib/document/extractor/plain/SourceDocInfoExtractor.h"
#include "indexlib/document/extractor/plain/SummaryDocInfoExtractor.h"

using namespace indexlibv2::document::extractor;

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.document, DocumentInfoExtractorFactory);

std::unique_ptr<IDocumentInfoExtractor>
DocumentInfoExtractorFactory::CreateDocumentInfoExtractor(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                          DocumentInfoExtractorType docExtractorType,
                                                          std::any& fieldHint)
{
    using namespace indexlibv2::document::extractor;
    switch (docExtractorType) {
    case DocumentInfoExtractorType::PRIMARY_KEY:
        return std::make_unique<PrimaryKeyInfoExtractor>(indexConfig);
    case DocumentInfoExtractorType::ATTRIBUTE_DOC:
        return std::make_unique<AttrDocInfoExtractor>();
    case DocumentInfoExtractorType::ATTRIBUTE_FIELD: {
        if (fieldid_t* fieldId = std::any_cast<fieldid_t>(&fieldHint)) {
            return std::make_unique<AttrFieldInfoExtractor>(*fieldId);
        } else {
            AUTIL_LOG(ERROR, "un-support filed type: [docExtractorType: %s, FiledType: %s]",
                      DocumentInfoExtractorTypeToStr(docExtractorType).c_str(), fieldHint.type().name());
        }
        return nullptr;
    }
    case DocumentInfoExtractorType::PACK_ATTRIBUTE_FIELD: {
        if (packattrid_t* packAttrId = std::any_cast<packattrid_t>(&fieldHint)) {
            return std::make_unique<PackAttrFieldInfoExtractor>(*packAttrId);
        } else {
            AUTIL_LOG(ERROR, "un-support filed type: [docExtractorType: %s, FiledType: %s]",
                      DocumentInfoExtractorTypeToStr(docExtractorType).c_str(), fieldHint.type().name());
        }
        return nullptr;
    }
    case DocumentInfoExtractorType::INVERTED_INDEX_DOC:
        return std::make_unique<InvertedIndexDocInfoExtractor>();
    case DocumentInfoExtractorType::SUMMARY_DOC:
        return std::make_unique<SummaryDocInfoExtractor>(indexConfig);
    case DocumentInfoExtractorType::SOURCE_DOC:
        return std::make_unique<SourceDocInfoExtractor>(indexConfig);
    case DocumentInfoExtractorType::FIELD_META_FIELD:
        if (fieldid_t* fieldId = std::any_cast<fieldid_t>(&fieldHint)) {
            return std::make_unique<indexlib::plain::FieldMetaFieldInfoExtractor>(*fieldId);
        } else {
            AUTIL_LOG(ERROR, "un-support filed type: [docExtractorType: %s, FiledType: %s]",
                      DocumentInfoExtractorTypeToStr(docExtractorType).c_str(), fieldHint.type().name());
        }
        return nullptr;

    default:
        return nullptr;
    }
    return nullptr;
}

} // namespace indexlibv2::plain
