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
#include "indexlib/document/raw_document/DefaultRawDocument.h"

#include <algorithm>
#include <unistd.h>

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/document/RawDocumentDefine.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, DefaultRawDocument);

std::string DefaultRawDocument::RAW_DOC_IDENTIFIER = "__@__default_raw_documment";

const autil::StringView DefaultRawDocument::EMPTY_STRING;

DefaultRawDocument::DefaultRawDocument(const DefaultRawDocument& other)
    : RawDocument(other)
    , _hashMapManager(other._hashMapManager)
    , _hashMapPrimary(other._hashMapPrimary)              // primary: shallow copy
    , _hashMapIncrement(other._hashMapIncrement->clone()) // increment: deep copy
    , _fieldCount(other._fieldCount)
    , _opType(other._opType)
    , _timestamp(other._timestamp)
    , _tagInfo(other._tagInfo)
    , _locator(other._locator)
    , _docInfo(other._docInfo)
{
    // all the ConstStrings of value: deep copy
    _fieldsPrimary.reserve(other._fieldsPrimary.size());
    for (FieldVec::const_iterator it = other._fieldsPrimary.begin(); it != other._fieldsPrimary.end(); ++it) {
        if (it->data()) {
            StringView fieldValue = autil::MakeCString(*it, _pool.get());
            _fieldsPrimary.emplace_back(fieldValue);
        } else {
            _fieldsPrimary.emplace_back(StringView());
        }
    }
    _fieldsIncrement.reserve(other._fieldsIncrement.size());
    for (FieldVec::const_iterator it = other._fieldsIncrement.begin(); it != other._fieldsIncrement.end(); ++it) {
        if (it->data()) {
            StringView fieldValue = autil::MakeCString(*it, _pool.get());
            _fieldsIncrement.emplace_back(fieldValue);
        } else {
            _fieldsIncrement.emplace_back(StringView());
        }
    }
}

void DefaultRawDocument::extractFieldNames(vector<StringView>& fieldNames) const
{
    if (_hashMapPrimary) {
        const FieldVec& keyFields1 = _hashMapPrimary->getKeyFields();
        assert(keyFields1.size() == _fieldsPrimary.size());
        fieldNames.reserve(_fieldsPrimary.size());
        for (size_t i = 0; i < _fieldsPrimary.size(); ++i) {
            if (_fieldsPrimary[i].data() && keyFields1[i].data()) {
                fieldNames.emplace_back(keyFields1[i]);
            }
        }
    }

    const FieldVec& keyFields2 = _hashMapIncrement->getKeyFields();
    assert(keyFields2.size() == _fieldsIncrement.size());
    fieldNames.reserve(_fieldsIncrement.size());
    for (size_t i = 0; i < _fieldsIncrement.size(); ++i) {
        if (_fieldsIncrement[i].data() && keyFields2[i].data()) {
            fieldNames.emplace_back(keyFields2[i]);
        }
    }
}

string DefaultRawDocument::toString() const
{
    stringstream ss;
    if (_hashMapPrimary) {
        const FieldVec& keyFields1 = _hashMapPrimary->getKeyFields();
        assert(keyFields1.size() == _fieldsPrimary.size());
        for (size_t i = 0; i < _fieldsPrimary.size(); ++i) {
            if (_fieldsPrimary[i].data() && keyFields1[i].data()) {
                ss << keyFields1[i] << " = " << _fieldsPrimary[i] << endl;
            }
        }
    }

    const FieldVec& keyFields2 = _hashMapIncrement->getKeyFields();
    assert(keyFields2.size() == _fieldsIncrement.size());
    for (size_t i = 0; i < _fieldsIncrement.size(); ++i) {
        if (_fieldsIncrement[i].data() && keyFields2[i].data()) {
            ss << keyFields2[i] << " = " << _fieldsIncrement[i] << endl;
        }
    }
    return ss.str();
}

DefaultRawDocument* DefaultRawDocument::clone() const { return new DefaultRawDocument(*this); }

DefaultRawDocument* DefaultRawDocument::createNewDocument() const { return new DefaultRawDocument(_hashMapManager); }

void DefaultRawDocument::setField(const StringView& fieldName, const StringView& fieldValue)
{
    StringView copyedValue = autil::MakeCString(fieldValue, _pool.get());
    StringView* value = search(fieldName);
    if (value) {
        // if the KEY is in the map, then just record the VALUE.
        if (value->data() == NULL) {
            _fieldCount++;
        }
        *value = copyedValue;
    } else {
        // or record both KEY and VALUE.
        addNewField(fieldName, copyedValue);
    }
}

void DefaultRawDocument::setFieldNoCopy(const StringView& fieldName, const StringView& fieldValue)
{
    assert(_pool->isInPool(fieldValue.data()));
    StringView* value = search(fieldName);
    if (value) {
        if (value->data() == NULL) {
            _fieldCount++;
        }
        *value = fieldValue;
    } else {
        addNewField(fieldName, fieldValue);
    }
}

const StringView& DefaultRawDocument::getField(const StringView& fieldName) const
{
    const StringView* fieldValue = search(fieldName);
    if (fieldValue == NULL || fieldValue->data() == NULL) {
        return EMPTY_STRING;
    }
    return *fieldValue;
}

// search: return NULL if the KEY haven't been seen.
// Note that because sharing hash map,
// returning a valid pointer does NOT mean the VALUE is set in this doc.
StringView* DefaultRawDocument::search(const StringView& fieldName)
{
    size_t id = KeyMap::INVALID_INDEX;
    if (_hashMapPrimary) {
        id = _hashMapPrimary->find(fieldName);
        if (id < _fieldsPrimary.size()) {
            return &_fieldsPrimary[id];
        }
    }

    id = _hashMapIncrement->find(fieldName);
    if (id < _fieldsIncrement.size()) {
        return &_fieldsIncrement[id];
    }
    return NULL;
}

const StringView* DefaultRawDocument::search(const StringView& fieldName) const
{
    size_t id = KeyMap::INVALID_INDEX;
    if (_hashMapPrimary) {
        id = _hashMapPrimary->find(fieldName);
        if (id < _fieldsPrimary.size()) {
            return &_fieldsPrimary[id];
        }
    }

    id = _hashMapIncrement->find(fieldName);
    if (id < _fieldsIncrement.size()) {
        return &_fieldsIncrement[id];
    }
    return NULL;
}

void DefaultRawDocument::addNewField(const StringView& fieldName, const StringView& fieldValue)
{
    _hashMapIncrement->insert(fieldName);
    _fieldsIncrement.emplace_back(fieldValue);
    _fieldCount++;
}

bool DefaultRawDocument::exist(const StringView& fieldName) const
{
    const StringView* ptr = search(StringView(fieldName));
    return ptr != NULL && ptr->data() != NULL;
}

void DefaultRawDocument::eraseField(const StringView& fieldName)
{
    StringView* value = search(fieldName);
    if (value) {
        *value = EMPTY_STRING;
        _fieldCount--;
    }
}

uint32_t DefaultRawDocument::getFieldCount() const { return _fieldCount; }

void DefaultRawDocument::clear()
{
    _fieldCount = 0;
    _hashMapPrimary = _hashMapManager->getHashMapPrimary();
    _fieldsPrimary.clear();
    if (_hashMapPrimary) {
        _fieldsPrimary.resize(_hashMapPrimary->size());
    }
    _fieldsIncrement.clear();
    _hashMapIncrement.reset(new KeyMap());
}

DocOperateType DefaultRawDocument::getDocOperateType() const
{
    if (_opType != UNKNOWN_OP) {
        return _opType;
    }
    const StringView& cmd = getField(StringView(CMD_TAG));
    _opType = RawDocument::getDocOperateType(cmd);
    return _opType;
}

RawDocFieldIterator* DefaultRawDocument::CreateIterator() const
{
    const FieldVec* fieldsKeyPrimary = NULL;
    if (_hashMapPrimary) {
        fieldsKeyPrimary = &(_hashMapPrimary->getKeyFields());
    }
    const FieldVec* fieldsKeyIncrement = &(_hashMapIncrement->getKeyFields());
    return new DefaultRawDocFieldIterator(fieldsKeyPrimary, &_fieldsPrimary, fieldsKeyIncrement, &_fieldsIncrement);
}

void DefaultRawDocument::AddTag(const string& key, const string& value) { _tagInfo[key] = value; }

const string& DefaultRawDocument::GetTag(const string& key) const
{
    auto iter = _tagInfo.find(key);
    if (iter != _tagInfo.end()) {
        return iter->second;
    }
    static string emptyString;
    return emptyString;
}

void DefaultRawDocument::SetIngestionTimestamp(int64_t ingestionTimestamp)
{
    AddTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY, std::to_string(ingestionTimestamp));
}
int64_t DefaultRawDocument::GetIngestionTimestamp() const
{
    const auto& ingestionTimestampStr = GetTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY);
    if (ingestionTimestampStr.empty()) {
        return INVALID_TIMESTAMP;
    }
    return autil::StringUtil::fromString<int64_t>(ingestionTimestampStr);
}

void DefaultRawDocument::SetDocInfo(const indexlibv2::document::IDocument::DocInfo& docInfo) { _docInfo = docInfo; }
indexlibv2::document::IDocument::DocInfo DefaultRawDocument::GetDocInfo() const { return _docInfo; }
size_t DefaultRawDocument::EstimateMemory() const
{
    size_t ret = sizeof(*this);
    if (_pool) {
        size_t pageSize = getpagesize();
        size_t pageAlignSize = (_pool->getUsedBytes() + pageSize - 1) / pageSize * pageSize;
        ret += pageAlignSize;
    }
    return ret;
}

}} // namespace indexlibv2::document
