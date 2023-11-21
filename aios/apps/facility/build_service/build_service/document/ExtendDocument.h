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

#include <assert.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/any.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/document/TokenizeDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace document {

enum class ProcessorWarningFlag {
    PWF_HASH_FIELD_EMPTY = 0x1,
    PWF_ATTR_CONVERT_ERROR = 0x2,
    PWF_PROCESS_FAIL_IN_BATCH = 0x4,
};

class ExtendDocument;

typedef std::shared_ptr<ExtendDocument> ExtendDocumentPtr;

class ExtendDocument
{
public:
    static const std::string INDEX_SCHEMA;
    static const std::string DOC_OPERATION_TYPE;
    typedef std::map<fieldid_t, std::string> FieldAnalyzerNameMap;
    typedef std::vector<ExtendDocumentPtr> ExtendDocumentVec;

public:
    ExtendDocument();
    ExtendDocument(const std::shared_ptr<indexlibv2::document::ExtendDocument>& extendDoc);
    ~ExtendDocument();

private:
    ExtendDocument(const ExtendDocument&);
    ExtendDocument& operator=(const ExtendDocument&);

public:
    void setRawDocument(const RawDocumentPtr& doc) { _indexExtendDoc->SetRawDocument(doc); }

    const RawDocumentPtr& getRawDocument() const { return _indexExtendDoc->GetRawDocument(); }

    TokenizeDocumentPtr getTokenizeDocument() const;

    TokenizeDocumentPtr getLastTokenizeDocument() const;

    ClassifiedDocumentPtr getClassifiedDocument() const;

    const ProcessedDocumentPtr& getProcessedDocument() const { return _processedDocument; }

    void addSubDocument(ExtendDocumentPtr& document) { _subDocuments.push_back(document); }

    ExtendDocumentPtr& getSubDocument(size_t index)
    {
        if (index >= _subDocuments.size()) {
            static ExtendDocumentPtr emptyExtendDocument;
            return emptyExtendDocument;
        }
        return _subDocuments[index];
    }

    void removeSubDocument(size_t index)
    {
        if (index < _subDocuments.size()) {
            _subDocuments.erase(_subDocuments.begin() + index);
        }
    }

    size_t getSubDocumentsCount() { return _subDocuments.size(); }

    const ExtendDocumentVec& getAllSubDocuments() const { return _subDocuments; }

    void setProperty(const std::string& key, autil::legacy::Any value) { _properties[key] = value; }

    autil::legacy::Any getProperty(const std::string& key) const
    {
        std::map<std::string, autil::legacy::Any>::const_iterator it = _properties.find(key);
        if (it != _properties.end()) {
            return it->second;
        }
        return autil::legacy::Any();
    }

    void setFieldAnalyzerName(fieldid_t fieldId, const std::string& analyzerName)
    {
        _fieldAnalyzerNameMap[fieldId] = analyzerName;
    }
    std::string getFieldAnalyzerName(fieldid_t fieldId) const
    {
        FieldAnalyzerNameMap::const_iterator it;
        it = _fieldAnalyzerNameMap.find(fieldId);
        if (it != _fieldAnalyzerNameMap.end()) {
            return it->second;
        } else {
            return "";
        }
    }
    void setFieldAnalyzerNameMap(FieldAnalyzerNameMap fieldAnalyzerNameMap)
    {
        _fieldAnalyzerNameMap = fieldAnalyzerNameMap;
    }
    regionid_t getRegionId() const
    {
        auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
        if (legacyExtendDoc) {
            return legacyExtendDoc->getRegionId();
        }
        return DEFAULT_REGIONID;
    }
    void setRegionId(regionid_t regionId)
    {
        auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
        if (legacyExtendDoc) {
            return legacyExtendDoc->setRegionId(regionId);
        }
        assert(0);
    }

    void setIdentifier(const std::string& identifier)
    {
        auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
        if (legacyExtendDoc) {
            return legacyExtendDoc->setIdentifier(identifier);
        }
        auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
        if (extendDoc) {
            return extendDoc->setIdentifier(identifier);
        }
        assert(0);
    }

    const std::string& getIdentifier() const
    {
        auto legacyExtendDoc = std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
        if (legacyExtendDoc) {
            return legacyExtendDoc->getIdentifier();
        }
        auto extendDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalExtendDocument>(_indexExtendDoc);
        if (extendDoc) {
            return extendDoc->getIdentifier();
        }
        static std::string empty;
        return empty;
    }

    // user-processor can set raw document string, which will be set to processedDocument
    void setRawDocString(const std::string& docStr) { _rawDocStr = docStr; }
    const std::string& getRawDocString() const { return _rawDocStr; }
    std::shared_ptr<indexlibv2::document::RawDocument::Snapshot> getOriginalSnapshot() const;
    void setOriginalSnapshot(const std::shared_ptr<indexlibv2::document::RawDocument::Snapshot>& rawDocument);

    void setWarningFlag(ProcessorWarningFlag warningFlag)
    {
        _buildInWarningFlags |= static_cast<uint32_t>(warningFlag);
    }

    bool testWarningFlag(ProcessorWarningFlag warningFlag) const
    {
        uint32_t testFlag = static_cast<uint32_t>(warningFlag) & _buildInWarningFlags;
        return static_cast<uint32_t>(warningFlag) == testFlag;
    }

    indexlib::document::IndexlibExtendDocumentPtr getLegacyExtendDoc() const
    {
        return std::dynamic_pointer_cast<indexlib::document::IndexlibExtendDocument>(_indexExtendDoc);
    }

    const std::shared_ptr<indexlibv2::document::ExtendDocument>& getExtendDoc() const { return _indexExtendDoc; }

    const FieldAnalyzerNameMap& getFieldAnalyzerNameMap() const { return _fieldAnalyzerNameMap; }

    void addModifiedField(fieldid_t fid);
    bool isModifiedFieldSetEmpty() const;
    void clearModifiedFieldSet();
    bool hasFieldInModifiedFieldSet(fieldid_t fid) const;
    void removeModifiedField(fieldid_t fid);

private:
    std::shared_ptr<indexlibv2::document::ExtendDocument> _indexExtendDoc;
    ProcessedDocumentPtr _processedDocument;
    ExtendDocumentVec _subDocuments;
    std::map<std::string, autil::legacy::Any> _properties;
    FieldAnalyzerNameMap _fieldAnalyzerNameMap;
    std::string _rawDocStr;
    uint32_t _buildInWarningFlags = 0;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::document
