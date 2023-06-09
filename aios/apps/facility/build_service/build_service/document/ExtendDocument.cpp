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
#include "build_service/document/ExtendDocument.h"

#include "ExtendDocument.h"
#include "indexlib/document/normal/NormalExtendDocument.h"

using namespace std;
using namespace indexlib::document;

namespace build_service { namespace document {
BS_LOG_SETUP(document, ExtendDocument);

const string ExtendDocument::INDEX_SCHEMA = "index_schema";
const string ExtendDocument::DOC_OPERATION_TYPE = "doc_operation_type";

ExtendDocument::ExtendDocument()
{
    _indexExtendDoc.reset(new IndexlibExtendDocument);
    _processedDocument.reset(new ProcessedDocument());
}

ExtendDocument::ExtendDocument(const std::shared_ptr<indexlibv2::document::ExtendDocument>& extendDoc)
{
    assert(extendDoc);
    _indexExtendDoc = extendDoc;
    _processedDocument.reset(new ProcessedDocument());
}

ExtendDocument::~ExtendDocument() {}

#define ADAPTIVE_EXTEND_DOC_FUNC(funcName)                                                                             \
    do {                                                                                                               \
        auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc); \
        if (legacyExtendDoc) {                                                                                         \
            return legacyExtendDoc->funcName();                                                                        \
        }                                                                                                              \
        auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);       \
        if (extendDoc) {                                                                                               \
            return extendDoc->funcName();                                                                              \
        }                                                                                                              \
        return nullptr;                                                                                                \
    } while (0)

TokenizeDocumentPtr ExtendDocument::getTokenizeDocument() const { ADAPTIVE_EXTEND_DOC_FUNC(getTokenizeDocument); }

ClassifiedDocumentPtr ExtendDocument::getClassifiedDocument() const { ADAPTIVE_EXTEND_DOC_FUNC(getClassifiedDocument); }

TokenizeDocumentPtr ExtendDocument::getLastTokenizeDocument() const
{
    ADAPTIVE_EXTEND_DOC_FUNC(getLastTokenizeDocument);
}

#undef ADAPTIVE_EXTEND_DOC_FUNC

void ExtendDocument::addModifiedField(fieldid_t fid)
{
    auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    if (legacyExtendDoc) {
        legacyExtendDoc->addModifiedField(fid);
        return;
    }
    auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
    if (extendDoc) {
        extendDoc->addModifiedField(fid);
        return;
    }
    assert(0);
}

bool ExtendDocument::isModifiedFieldSetEmpty() const
{
    auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    if (legacyExtendDoc) {
        return legacyExtendDoc->isModifiedFieldSetEmpty();
    }
    auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
    if (extendDoc) {
        return extendDoc->isModifiedFieldSetEmpty();
    }
    assert(0);
    return true;
}

void ExtendDocument::clearModifiedFieldSet()
{
    auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    if (legacyExtendDoc) {
        legacyExtendDoc->clearModifiedFieldSet();
        return;
    }
    auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
    if (extendDoc) {
        extendDoc->clearModifiedFieldSet();
        return;
    }
    assert(0);
}

bool ExtendDocument::hasFieldInModifiedFieldSet(fieldid_t fid) const
{
    auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    if (legacyExtendDoc) {
        return legacyExtendDoc->hasFieldInModifiedFieldSet(fid);
    }
    auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
    if (extendDoc) {
        return extendDoc->hasFieldInModifiedFieldSet(fid);
    }
    assert(0);
    return false;
}

void ExtendDocument::removeModifiedField(fieldid_t fid)
{
    auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    if (legacyExtendDoc) {
        legacyExtendDoc->removeModifiedField(fid);
        return;
    }
    auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
    if (extendDoc) {
        extendDoc->removeModifiedField(fid);
        return;
    }
    assert(0);
}

}} // namespace build_service::document
