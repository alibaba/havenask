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
#include <memory>

#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/ClassifiedDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"

namespace indexlibv2 { namespace document {

class NormalExtendDocument : public ExtendDocument
{
public:
    typedef std::vector<std::shared_ptr<NormalExtendDocument>> NormalExtendDocumentVec;
    typedef std::set<fieldid_t> FieldIdSet;

public:
    NormalExtendDocument()
    {
        _tokenizeDocument.reset(new indexlib::document::TokenizeDocument());
        _lastTokenizeDocument.reset(new indexlib::document::TokenizeDocument());
        _classifiedDocument.reset(new ClassifiedDocument());
    }

    ~NormalExtendDocument() {}

public:
    const std::shared_ptr<indexlib::document::TokenizeDocument>& getTokenizeDocument() const
    {
        return _tokenizeDocument;
    }

    const std::shared_ptr<indexlib::document::TokenizeDocument>& getLastTokenizeDocument() const
    {
        return _lastTokenizeDocument;
    }
    const std::shared_ptr<ClassifiedDocument>& getClassifiedDocument() const { return _classifiedDocument; }

    void setIdentifier(const std::string& identifier) { _identifier = identifier; }
    const std::string& getIdentifier() const { return _identifier; }

public: // modified fields
    // deleted from raw document & move to here
    virtual bool isModifiedFieldSetEmpty() const;
    virtual void clearModifiedFieldSet();

public:
    const FieldIdSet& getModifiedFieldSet() const { return _modifiedFields; }
    void addModifiedField(fieldid_t fid) { _modifiedFields.insert(fid); }
    void removeModifiedField(fieldid_t fid) { _modifiedFields.erase(fid); }
    void setModifiedFieldSet(const FieldIdSet& modifiedFields) { _modifiedFields = modifiedFields; }
    bool hasFieldInModifiedFieldSet(fieldid_t fid) const;

private:
    std::shared_ptr<indexlib::document::TokenizeDocument> _tokenizeDocument;
    std::shared_ptr<indexlib::document::TokenizeDocument> _lastTokenizeDocument;
    std::shared_ptr<ClassifiedDocument> _classifiedDocument;
    FieldIdSet _modifiedFields;
    std::string _identifier;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////////
inline bool NormalExtendDocument::hasFieldInModifiedFieldSet(fieldid_t fid) const
{
    return _modifiedFields.count(fid) > 0;
}

inline bool NormalExtendDocument::isModifiedFieldSetEmpty() const { return _modifiedFields.empty(); }

inline void NormalExtendDocument::clearModifiedFieldSet() { _modifiedFields.clear(); }
}} // namespace indexlibv2::document
