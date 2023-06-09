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
#include "indexlib/document/normal/SerializedSourceDocument.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SerializedSourceDocument);

SerializedSourceDocument::SerializedSourceDocument(autil::mem_pool::Pool* pool)
    : _meta(autil::StringView::empty_instance())
{
}

SerializedSourceDocument::~SerializedSourceDocument() {}

void SerializedSourceDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write((uint32_t)_meta.size());
    if (_meta.size() > 0) {
        dataBuffer.writeBytes(_meta.data(), _meta.size());
    }

    dataBuffer.write((uint32_t)_data.size());
    for (size_t i = 0; i < _data.size(); i++) {
        dataBuffer.write((uint32_t)_data[i].size());
        if (_data[i].size() > 0) {
            dataBuffer.writeBytes(_data[i].data(), _data[i].size());
        }
    }
    dataBuffer.write((uint32_t)_accessaryMeta.size());
    if (_accessaryMeta.size() > 0) {
        dataBuffer.writeBytes(_accessaryMeta.data(), _accessaryMeta.size());
    }
    dataBuffer.write((uint32_t)_accessaryData.size());
    if (_accessaryData.size() > 0) {
        dataBuffer.writeBytes(_accessaryData.data(), _accessaryData.size());
    }
    dataBuffer.write((uint32_t)_nonExistFieldInfo.size());
    if (_nonExistFieldInfo.size() > 0) {
        dataBuffer.writeBytes(_nonExistFieldInfo.data(), _nonExistFieldInfo.size());
    }
}

void SerializedSourceDocument::deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                                           uint32_t docVersion)
{
    assert(docVersion >= 9);
    assert(pool);

    uint32_t metaLen = 0;
    dataBuffer.read(metaLen);
    if (metaLen > 0) {
        char* metaData = (char*)pool->allocate(metaLen);
        dataBuffer.readBytes(metaData, metaLen);
        _meta = autil::StringView(metaData, metaLen);
    } else {
        _meta = autil::StringView::empty_instance();
    }
    uint32_t groupCount = 0;
    dataBuffer.read(groupCount);
    _data.resize(groupCount);
    for (uint32_t i = 0; i < groupCount; i++) {
        uint32_t dataLen = 0;
        dataBuffer.read(dataLen);
        if (dataLen == 0) {
            _data[i] = autil::StringView::empty_instance();
            continue;
        }
        char* data = (char*)pool->allocate(dataLen);
        dataBuffer.readBytes(data, dataLen);
        _data[i] = autil::StringView(data, dataLen);
    }

    uint32_t accessaryMetaLen = 0;
    dataBuffer.read(accessaryMetaLen);
    if (accessaryMetaLen > 0) {
        char* accessaryMeta = (char*)pool->allocate(accessaryMetaLen);
        dataBuffer.readBytes(accessaryMeta, accessaryMetaLen);
        _accessaryMeta = {accessaryMeta, accessaryMetaLen};
    }

    uint32_t accessaryDataLen = 0;
    dataBuffer.read(accessaryDataLen);
    if (accessaryDataLen > 0) {
        char* accessaryData = (char*)pool->allocate(accessaryDataLen);
        dataBuffer.readBytes(accessaryData, accessaryDataLen);
        _accessaryData = {accessaryData, accessaryDataLen};
    }

    uint32_t nonExistFieldInfoLen = 0;
    dataBuffer.read(nonExistFieldInfoLen);
    if (nonExistFieldInfoLen > 0) {
        char* nonExistFieldInfoData = (char*)pool->allocate(nonExistFieldInfoLen);
        dataBuffer.readBytes(nonExistFieldInfoData, nonExistFieldInfoLen);
        _nonExistFieldInfo = {nonExistFieldInfoData, nonExistFieldInfoLen};
    }
}

const StringView SerializedSourceDocument::GetGroupValue(index::groupid_t groupId) const
{
    if (groupId >= _data.size()) {
        return StringView::empty_instance();
    }
    return _data[groupId];
}

void SerializedSourceDocument::AddGroupValue(StringView& value) { _data.push_back(value); }

const StringView SerializedSourceDocument::GetMeta() const { return _meta; }
void SerializedSourceDocument::SetMeta(StringView& meta) { _meta = meta; }

void SerializedSourceDocument::AddAccessaryGroupValue(const StringView& groupMeta, const StringView& groupValue)
{
    _accessaryMeta = groupMeta;
    _accessaryData = groupValue;
}
}} // namespace indexlib::document
