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
#include "indexlib/document/normal/SourceDocument.h"

#include <iostream>

#include "autil/mem_pool/Pool.h"
#include "indexlib/document/RawDocument.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, SourceDocument);

SourceDocument::SourceDocument(autil::mem_pool::PoolBase* pool)
    : _ownPool(pool == NULL)
    , _pool(pool ? pool : new Pool())
{
}

SourceDocument::~SourceDocument()
{
    if (_ownPool) {
        delete _pool;
        _pool = NULL;
    }
}

bool SourceDocument::operator==(const SourceDocument& right) const
{
    auto checkGroupEqual = [](const SourceGroupPtr& lft, const SourceGroupPtr& rht) {
        if ((lft && !rht) || (!lft && rht)) {
            return false;
        }
        if (!lft && !rht) {
            return true;
        }
        return (*lft) == (*rht);
    };

    if (_data.size() != right._data.size()) {
        return false;
    }
    for (size_t i = 0; i < _data.size(); i++) {
        if (!checkGroupEqual(_data[i], right._data[i])) {
            return false;
        }
    }
    return checkGroupEqual(_accessaryData, right._accessaryData);
}

bool SourceDocument::operator!=(const SourceDocument& right) const { return !operator==(right); }

const StringView& SourceDocument::GetField(index::groupid_t groupId, const string& fieldName) const
{
    if (groupId >= _data.size()) {
        return StringView::empty_instance();
    }
    return _data[groupId]->GetField(fieldName);
}

const autil::StringView& SourceDocument::GetAccessaryField(const std::string& fieldName) const
{
    if (!_accessaryData) {
        return StringView::empty_instance();
    }
    return _accessaryData->GetField(fieldName);
}

bool SourceDocument::HasField(index::groupid_t groupId, const string& fieldName) const
{
    if (groupId >= _data.size()) {
        return false;
    }
    return _data[groupId]->HasField(fieldName);
}

bool SourceDocument::IsNonExistField(index::groupid_t groupId, const string& fieldName) const
{
    if (groupId >= _data.size()) {
        return false;
    }
    return _data[groupId]->IsNonExistField(fieldName);
}

void SourceDocument::Append(const index::groupid_t groupId, const StringView& fieldName, const StringView& value,
                            bool needCopy)
{
    string fn(fieldName.data(), fieldName.size());
    Append(groupId, fn, value, needCopy);
}

void SourceDocument::AppendNonExistField(index::groupid_t groupId, const StringView& fieldName)
{
    string fn(fieldName.data(), fieldName.size());
    AppendNonExistField(groupId, fn);
}

void SourceDocument::AppendNonExistField(index::groupid_t groupId, const string& fieldName)
{
    if (groupId < 0) {
        return;
    }
    while (groupId >= _data.size()) {
        _data.push_back(SourceGroupPtr(new SourceGroup));
    }
    _data[groupId]->AppendNonExistField(fieldName);
}

void SourceDocument::Append(const index::groupid_t groupId, const string& fieldName, const StringView& value,
                            bool needCopy)
{
    if (groupId < 0) {
        // add log
        return;
    }
    while (groupId >= _data.size()) {
        _data.push_back(SourceGroupPtr(new SourceGroup));
    }

    if (!needCopy) {
        _data[groupId]->Append(fieldName, value);
        return;
    }
    StringView copyValue = autil::MakeCString(value, _pool);
    _data[groupId]->Append(fieldName, copyValue);
    return;
}

void SourceDocument::AppendNonExistAccessaryField(const string& fieldName)
{
    if (!_accessaryData) {
        _accessaryData.reset(new SourceGroup);
    }
    _accessaryData->AppendNonExistField(fieldName);
}

void SourceDocument::AppendAccessaryField(const string& fieldName, const StringView& value, bool needCopy)
{
    if (!_accessaryData) {
        _accessaryData.reset(new SourceGroup);
    }

    if (!needCopy) {
        _accessaryData->Append(fieldName, value);
        return;
    }
    StringView copyValue = autil::MakeCString(value, _pool);
    _accessaryData->Append(fieldName, copyValue);
}

void SourceDocument::AppendAccessaryField(const StringView& fieldName, const StringView& value, bool needCopy)
{
    string fn(fieldName.data(), fieldName.size());
    AppendAccessaryField(fn, value, needCopy);
}

void SourceDocument::SourceGroup::Append(const string& fieldName, const StringView& value)
{
    auto iter = this->fieldsMap.find(fieldName);
    if (iter != this->fieldsMap.end()) {
        this->fieldsValue[iter->second] = value;
        return;
    }
    this->fieldsValue.push_back(value);
    this->fieldsName.push_back(fieldName);
    this->fieldsMap.insert(make_pair(fieldName, this->fieldsValue.size() - 1));
    return;
}

void SourceDocument::SourceGroup::AppendNonExistField(const string& fieldName)
{
    Append(fieldName, autil::StringView::empty_instance());
    this->nonExistFieldIdSet.insert(this->fieldsMap[fieldName]);
}

bool SourceDocument::SourceGroup::operator==(const SourceGroup& right) const
{
    return this->fieldsValue == right.fieldsValue && this->fieldsName == right.fieldsName &&
           this->nonExistFieldIdSet == right.nonExistFieldIdSet;
}

bool SourceDocument::SourceGroup::operator!=(const SourceGroup& right) const { return !operator==(right); }

const StringView& SourceDocument::SourceGroup::GetField(const string& fieldName) const
{
    auto iter = this->fieldsMap.find(fieldName);
    if (iter != this->fieldsMap.end()) {
        int64_t index = iter->second;
        assert(index >= 0 && index < this->fieldsValue.size());
        return this->fieldsValue[index];
    }
    return StringView::empty_instance();
}

bool SourceDocument::SourceGroup::HasField(const string& fieldName) const
{
    return this->fieldsMap.find(fieldName) != this->fieldsMap.end();
}

bool SourceDocument::SourceGroup::IsNonExistField(const string& fieldName) const
{
    auto iter = this->fieldsMap.find(fieldName);
    if (iter == this->fieldsMap.end()) {
        return false;
    }
    return IsNonExistField(iter->second);
}

bool SourceDocument::SourceGroup::IsNonExistField(size_t fieldIdx) const
{
    if (fieldIdx >= this->fieldsName.size()) {
        return false;
    }
    if (!this->fieldsValue[fieldIdx].empty()) {
        return false;
    }
    return this->nonExistFieldIdSet.find((uint16_t)fieldIdx) != this->nonExistFieldIdSet.end();
}

SourceDocument::SourceGroupFieldIter SourceDocument::CreateGroupFieldIter(index::groupid_t groupId) const
{
    if (groupId >= _data.size()) {
        return SourceDocument::SourceGroupFieldIter();
    }
    return SourceDocument::SourceGroupFieldIter(_data[groupId]);
}

SourceDocument::SourceMetaIter SourceDocument::CreateGroupMetaIter(index::groupid_t groupId) const
{
    if (groupId >= _data.size()) {
        return SourceDocument::SourceMetaIter();
    }
    return SourceDocument::SourceMetaIter(_data[groupId]);
}

SourceDocument::SourceGroupFieldIter SourceDocument::CreateAccessaryFieldIter() const
{
    if (_accessaryData) {
        return SourceDocument::SourceGroupFieldIter(_accessaryData);
    }
    return SourceDocument::SourceGroupFieldIter();
}

SourceDocument::SourceMetaIter SourceDocument::CreateAccessaryMetaIter() const
{
    if (_accessaryData) {
        return SourceDocument::SourceMetaIter(_accessaryData);
    }
    return SourceDocument::SourceMetaIter();
}

bool SourceDocument::SourceGroupFieldIter::HasNext() const { return _cursor < _size; }

const StringView SourceDocument::SourceGroupFieldIter::Next()
{
    const StringView ret = _sourceGroup->fieldsValue[_cursor];
    _cursor++;
    return ret;
}

bool SourceDocument::SourceMetaIter::HasNext() const { return _cursor < _size; }

const StringView SourceDocument::SourceMetaIter::Next()
{
    const StringView ret = StringView(_sourceGroup->fieldsName[_cursor]);
    _cursor++;
    return ret;
}

void SourceDocument::ToRawDocument(indexlibv2::document::RawDocument& rawDoc) const
{
    for (auto& sourceGroup : _data) {
        assert(sourceGroup);
        for (size_t i = 0; i < sourceGroup->fieldsName.size(); i++) {
            if (sourceGroup->IsNonExistField(i)) {
                continue;
            }
            StringView fieldName(sourceGroup->fieldsName[i]);
            rawDoc.setField(fieldName, sourceGroup->fieldsValue[i]);
        }
    }

    if (_accessaryData) {
        for (size_t i = 0; i < _accessaryData->fieldsName.size(); i++) {
            if (_accessaryData->IsNonExistField(i)) {
                continue;
            }
            StringView fieldName(_accessaryData->fieldsName[i]);
            rawDoc.setField(fieldName, _accessaryData->fieldsValue[i]);
        }
    }
}

void SourceDocument::ExtractFields(vector<string>& fieldNames, vector<string>& fieldValues,
                                   const set<string>& requiredFields) const
{
    bool hasRequiredField = !requiredFields.empty();

    assert(fieldNames.size() == fieldValues.size());
    for (auto& sourceGroup : _data) {
        assert(sourceGroup);
        for (size_t i = 0; i < sourceGroup->fieldsName.size(); i++) {
            if (sourceGroup->IsNonExistField(i)) {
                continue;
            }

            if (hasRequiredField && requiredFields.count(sourceGroup->fieldsName[i]) == 0) {
                continue;
            }
            fieldNames.push_back(sourceGroup->fieldsName[i]);
            string fieldValue(sourceGroup->fieldsValue[i].data(), sourceGroup->fieldsValue[i].size());
            fieldValues.push_back(fieldValue);
        }
    }

    if (_accessaryData) {
        for (size_t i = 0; i < _accessaryData->fieldsName.size(); i++) {
            if (_accessaryData->IsNonExistField(i)) {
                continue;
            }

            if (hasRequiredField && requiredFields.count(_accessaryData->fieldsName[i]) == 0) {
                continue;
            }
            fieldNames.push_back(_accessaryData->fieldsName[i]);
            string fieldValue(_accessaryData->fieldsValue[i].data(), _accessaryData->fieldsValue[i].size());
            fieldValues.push_back(fieldValue);
        }
    }
}

bool SourceDocument::Merge(const std::shared_ptr<SourceDocument>& other)
{
    // check before merge
    if (other->_data.size() != _data.size()) {
        return false;
    }
    for (size_t groupId = 0; groupId < other->_data.size(); groupId++) {
        if (_data[groupId]->fieldsName != other->_data[groupId]->fieldsName) {
            return false;
        }
    }

    // check done, do merge
    for (size_t groupId = 0; groupId < other->_data.size(); groupId++) {
        for (size_t fieldId = 0; fieldId < other->_data[groupId]->fieldsValue.size(); fieldId++) {
            assert(_data[groupId]->fieldsName[fieldId] == other->_data[groupId]->fieldsName[fieldId]);
            const autil::StringView& fieldValue = other->_data[groupId]->fieldsValue[fieldId];
            if (other->_data[groupId]->IsNonExistField(fieldId)) {
                continue;
            }

            if (_pool == other->_pool) {
                _data[groupId]->fieldsValue[fieldId] = fieldValue;
            } else {
                _data[groupId]->fieldsValue[fieldId] = autil::MakeCString(fieldValue, _pool);
            }
        }
    }

    if (other->_accessaryData) {
        for (size_t i = 0; i < other->_accessaryData->fieldsName.size(); i++) {
            StringView fieldValue = other->_accessaryData->fieldsValue[i];
            if (other->_accessaryData->IsNonExistField(i)) {
                continue;
            }

            StringView fieldName(other->_accessaryData->fieldsName[i]);
            if (_pool == other->_pool) {
                AppendAccessaryField(fieldName, fieldValue, false);
            } else {
                AppendAccessaryField(fieldName, fieldValue, true);
            }
        }
    }
    return true;
}

size_t SourceDocument::GetGroupCount() const { return _data.size(); }

autil::mem_pool::PoolBase* SourceDocument::GetPool() { return _pool; }

StringView SourceDocument::EncodeNonExistFieldInfo(autil::mem_pool::PoolBase* pool)
{
    uint32_t totalFieldCount = 0;
    bool hasNonExistField = false;
    for (auto data : _data) {
        if (!data->nonExistFieldIdSet.empty()) {
            hasNonExistField = true;
        }
        totalFieldCount += data->fieldsName.size();
    }

    if (_accessaryData) {
        totalFieldCount += _accessaryData->fieldsName.size();
    }

    if (!hasNonExistField) {
        char* data = (char*)pool->allocate(1);
        data[0] = char(0); // first byte = 0, means all field exist
        return StringView(data, 1);
    }

    size_t byteLen = (totalFieldCount + 7) / 8;
    char* data = (char*)pool->allocate(byteLen + 1);
    memset(data, 0, byteLen + 1);
    data[0] = char(1); // first byte = 1, means has non exist field

    static char SRC_BIT_MASKS[8] = {(char)0x80, (char)0x40, (char)0x20, (char)0x10,
                                    (char)0x08, (char)0x04, (char)0x02, (char)0x01};
    size_t baseId = 0;
    for (auto gdata : _data) {
        for (auto id : gdata->nonExistFieldIdSet) {
            size_t targetId = id + baseId;
            size_t bytePos = targetId / 8;
            size_t inBytePos = targetId % 8;
            data[bytePos + 1] |= SRC_BIT_MASKS[inBytePos];
        }
        baseId += gdata->fieldsName.size();
    }
    if (_accessaryData) {
        for (auto id : _accessaryData->nonExistFieldIdSet) {
            size_t targetId = id + baseId;
            size_t bytePos = targetId / 8;
            size_t inBytePos = targetId % 8;
            data[bytePos + 1] |= SRC_BIT_MASKS[inBytePos];
        }
    }
    return StringView(data, byteLen + 1);
}

Status SourceDocument::DecodeNonExistFieldInfo(const StringView& str)
{
    assert(!str.empty());
    const char* data = str.data();
    if (data[0] == char(0)) {
        return Status::OK();
    }

    uint32_t totalFieldCount = 0;
    for (auto data : _data) {
        totalFieldCount += data->fieldsName.size();
    }
    if (_accessaryData) {
        totalFieldCount += _accessaryData->fieldsName.size();
    }
    size_t byteLen = (totalFieldCount + 7) / 8;
    if (byteLen != str.size() - 1) {
        RETURN_IF_STATUS_ERROR(Status::Corruption("bitmap byteLen error"),
                               "expect bitmap byteLen [%lu] not match string len [%lu] - 1", byteLen, str.size());
    }

    assert(data[0] == char(1));
    static char SRC_BIT_MASKS[8] = {(char)0x80, (char)0x40, (char)0x20, (char)0x10,
                                    (char)0x08, (char)0x04, (char)0x02, (char)0x01};

    size_t groupId = 0;
    size_t baseId = 0;
    SourceGroupPtr groupData;
    for (size_t idx = 0; idx < totalFieldCount; idx++) {
        groupData = (groupId < _data.size()) ? _data[groupId] : _accessaryData;
        while ((idx - baseId) >= groupData->fieldsName.size()) {
            baseId += groupData->fieldsName.size();
            if (groupId < _data.size()) {
                ++groupId;
            }
            groupData = (groupId < _data.size()) ? _data[groupId] : _accessaryData;
        }
        size_t bytePos = idx / 8;
        size_t inBytePos = idx % 8;
        if ((data[bytePos + 1] & SRC_BIT_MASKS[inBytePos]) == 0) {
            continue;
        }
        size_t localId = idx - baseId;
        groupData->nonExistFieldIdSet.insert((uint16_t)localId);
    }
    return Status::OK();
}
} // namespace indexlib::document
