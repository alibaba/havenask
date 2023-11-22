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

#include <type_traits>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/IndexRawField.h"
#include "indexlib/document/normal/IndexTokenizeField.h"

namespace indexlibv2::config {
class SummaryIndexConfig;
class SourceIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::config {
class PayloadConfig;
}

namespace indexlib::document {
class AttributeDocument;
class FieldMetaDocument;
class IndexDocument;
class SerializedSummaryDocument;
class SerializedSourceDocument;
class SourceDocument;
class SummaryDocument;
} // namespace indexlib::document

namespace indexlibv2 { namespace document {
class RawDocument;

class ClassifiedDocument
{
public:
    const static uint32_t MAX_SECTION_LENGTH;
    const static uint32_t MAX_TOKEN_PER_SECTION;
    const static uint32_t MAX_SECTION_PER_FIELD;

    typedef autil::mem_pool::Pool PoolType;
    typedef std::shared_ptr<PoolType> PoolTypePtr;

public:
    ClassifiedDocument();
    ~ClassifiedDocument();

public:
    void setAttributeField(fieldid_t id, const std::string& value);
    void setAttributeField(fieldid_t id, const char* value, size_t len);
    void setAttributeFieldNoCopy(fieldid_t id, const autil::StringView& value);
    const autil::StringView& getAttributeField(fieldid_t fieldId);

    void setSummaryField(fieldid_t id, const std::string& value);
    void setSummaryField(fieldid_t id, const char* value, size_t len);
    void setSummaryFieldNoCopy(fieldid_t id, const autil::StringView& value);
    const autil::StringView& getSummaryField(fieldid_t fieldId);

    void setFieldMetaFieldNoCopy(fieldid_t id, const autil::StringView& value, bool isNull);

    termpayload_t getTermPayload(const std::string& termText) const;
    termpayload_t getTermPayload(uint64_t intKey) const;
    void setTermPayload(const std::string& termText, termpayload_t payload);
    void setTermPayload(uint64_t intKey, termpayload_t payload);

    docpayload_t getTermDocPayload(uint64_t intKey) const;
    void setTermDocPayload(uint64_t intKey, docpayload_t docPayload);
    [[deprecated("use setNamedTermDocPayload instead")]] docpayload_t
    getTermDocPayload(const std::string& termText) const;
    [[deprecated("use setNamedTermDocPayload instead")]] void setTermDocPayload(const std::string& termText,
                                                                                docpayload_t docPayload);
    docpayload_t getTermDocPayload(const indexlib::config::PayloadConfig& payloadConfig,
                                   const std::string& termText) const;
    void setTermDocPayload(const indexlib::config::PayloadConfig& payloadConfig, const std::string& termText,
                           docpayload_t docPayload);

    void setPrimaryKey(const std::string& primaryKey);
    const std::string& getPrimaryKey();

    void createSourceDocument(const std::vector<std::vector<std::string>>& fieldGroups,
                              const RawDocument::Snapshot* snapshot);

    void sourceDocumentAppendAccessaryFields(const std::vector<std::string>& fieldNames,
                                             const std::shared_ptr<RawDocument>& rawDoc);
    void createFieldMetaDocument();
    void setOriginalSnapshot(const std::shared_ptr<indexlibv2::document::RawDocument::Snapshot>& snapshot);
    std::shared_ptr<indexlibv2::document::RawDocument::Snapshot> getOriginalSnapshot() const;

    const std::shared_ptr<indexlib::document::SourceDocument>& getSourceDocument() const { return _srcDocument; }

    std::shared_ptr<indexlib::document::SerializedSourceDocument>
    getSerializedSourceDocument(const std::shared_ptr<config::SourceIndexConfig>& sourceIndexConfig,
                                autil::mem_pool::Pool* pool) const;

    // for srcNode
    void setSourceDocument(const std::shared_ptr<indexlib::document::SourceDocument>& sourceDocument)
    {
        _srcDocument = sourceDocument;
    }

public:
    // inner interface.
    indexlib::document::Field* createIndexField(fieldid_t fieldId, indexlib::document::Field::FieldTag fieldTag);

    static indexlib::document::Section* createSection(indexlib::document::IndexTokenizeField* field, uint32_t tokenNum,
                                                      section_weight_t sectionWeight);

    static section_len_t getMaxSectionLenght() { return MAX_SECTION_LENGTH; }

public:
    // for test
    const std::shared_ptr<indexlib::document::IndexDocument>& getIndexDocument() const { return _indexDocument; }
    const std::shared_ptr<indexlib::document::SummaryDocument>& getSummaryDoc() const { return _summaryDocument; }
    const std::shared_ptr<indexlib::document::FieldMetaDocument>& getFieldMetaDoc() const { return _fieldMetaDocument; }
    const std::shared_ptr<indexlib::document::AttributeDocument>& getAttributeDoc() const { return _attributeDocument; }
    const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& getSerSummaryDoc() const
    {
        return _serSummaryDoc;
    }
    void clear();

public:
    PoolType* getPool() const { return _pool.get(); }
    const PoolTypePtr& getPoolPtr() const { return _pool; }

    template <typename Formatter>
    auto serializeSummaryDocument(Formatter& formatter);

private:
    std::shared_ptr<indexlib::document::IndexDocument> _indexDocument;
    std::shared_ptr<indexlib::document::SummaryDocument> _summaryDocument;
    std::shared_ptr<indexlib::document::AttributeDocument> _attributeDocument;
    std::shared_ptr<indexlib::document::FieldMetaDocument> _fieldMetaDocument;
    std::shared_ptr<indexlib::document::SerializedSummaryDocument> _serSummaryDoc;
    std::shared_ptr<indexlib::document::SerializedSourceDocument> _serSourceDoc;
    std::shared_ptr<indexlib::document::SourceDocument> _srcDocument;
    std::shared_ptr<indexlibv2::document::RawDocument::Snapshot> _originalSnapshot;
    PoolTypePtr _pool;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////
template <typename Formatter>
inline auto ClassifiedDocument::serializeSummaryDocument(Formatter& formatter)
{
    return formatter.SerializeSummaryDoc(_summaryDocument, _serSummaryDoc);
}

}} // namespace indexlibv2::document
