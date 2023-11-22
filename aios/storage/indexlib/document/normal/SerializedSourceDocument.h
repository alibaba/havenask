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

#include "autil/DataBuffer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/index/source/Types.h"

namespace indexlib { namespace document {

class SerializedSourceDocument
{
public:
    SerializedSourceDocument(autil::mem_pool::Pool* pool = NULL);
    ~SerializedSourceDocument();

public:
    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);

    bool operator==(const SerializedSourceDocument& doc) const
    {
        return _meta == doc._meta && _data == doc._data && _accessaryMeta == doc._accessaryMeta &&
               _accessaryData == doc._accessaryData && _nonExistFieldInfo == doc._nonExistFieldInfo;
    }

    bool operator!=(const SerializedSourceDocument& doc) const { return !operator==(doc); }

public:
    autil::StringView GetMeta() const;
    autil::StringView GetGroupValue(index::sourcegroupid_t groupId) const;
    void SetGroupValue(index::sourcegroupid_t groupId, autil::StringView value);
    void SetMeta(const autil::StringView& meta);
    void AddAccessaryGroupValue(const autil::StringView& groupMeta, const autil::StringView& groupValue);

    const autil::StringView& GetAccessaryMeta() const { return _accessaryMeta; }
    const autil::StringView& GetAccessaryData() const { return _accessaryData; }

public:
    // don't delete meta & data, their lifecycle is managed outside
    autil::StringView _meta;
    std::vector<autil::StringView> _data;
    autil::StringView _accessaryMeta;
    autil::StringView _accessaryData;
    autil::StringView _nonExistFieldInfo;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
