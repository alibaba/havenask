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

#include <inttypes.h>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/normal/GroupFieldIter.h"
#include "indexlib/index/source/Types.h"

namespace autil::mem_pool {
class PoolBase;
}
namespace indexlibv2::document {
class RawDocument;
}

namespace indexlib::document {

class SourceDocument
{
public:
    SourceDocument(autil::mem_pool::PoolBase* pool);
    ~SourceDocument();

public:
    class SourceGroup
    {
    public:
        SourceGroup() {}
        ~SourceGroup() {}

    public:
        std::vector<autil::StringView> fieldsValue;
        // TODO use StringView
        std::vector<std::string> fieldsName;
        // from field name to field position
        std::unordered_map<std::string, int64_t> fieldsMap;
        std::set<uint16_t> nonExistFieldIdSet;

        void Append(const std::string& fieldName, const autil::StringView& value);
        void AppendNonExistField(const std::string& fieldName);

        bool operator==(const SourceGroup& right) const;
        bool operator!=(const SourceGroup& right) const;
        const autil::StringView& GetField(const std::string& fieldName) const;

        // is field defined in group
        bool HasField(const std::string& fieldName) const;

        // is field defined by not exist
        bool IsNonExistField(const std::string& fieldName) const;
        bool IsNonExistField(size_t fieldIdx) const;
    };
    typedef std::shared_ptr<SourceGroup> SourceGroupPtr;

    class SourceGroupFieldIter : public GroupFieldIter
    {
    public:
        SourceGroupFieldIter() : _cursor(0), _size(0) {}

        SourceGroupFieldIter(const SourceGroupPtr& sourceGroup)
            : _sourceGroup(sourceGroup)
            , _cursor(0)
            , _size(sourceGroup->fieldsValue.size())
        {
        }
        bool HasNext() const override;
        const autil::StringView Next() override;
        void Reset() { _cursor = 0; }

    private:
        SourceGroupPtr _sourceGroup;
        size_t _cursor;
        size_t _size;
    };

    class SourceMetaIter : public GroupFieldIter
    {
    public:
        SourceMetaIter() : _cursor(0), _size(0) {}

        SourceMetaIter(const SourceGroupPtr& sourceGroup)
            : _sourceGroup(sourceGroup)
            , _cursor(0)
            , _size(sourceGroup->fieldsName.size())
        {
        }
        bool HasNext() const override;
        const autil::StringView Next() override;
        void Reset() { _cursor = 0; }

    private:
        SourceGroupPtr _sourceGroup;
        size_t _cursor;
        size_t _size;
    };

public:
    // rawDoc -> sourceDoc, don't copy; serializedSourceDoc -> sourceDoc, copy value
    void Append(index::sourcegroupid_t groupId, const std::string& fieldName, const autil::StringView& value,
                bool needCopy);
    void Append(index::sourcegroupid_t groupId, const autil::StringView& fieldName, const autil::StringView& value,
                bool needCopy);

    void AppendNonExistField(index::sourcegroupid_t groupId, const std::string& fieldName);
    void AppendNonExistField(index::sourcegroupid_t groupId, const autil::StringView& fieldName);

    void AppendAccessaryField(const std::string& fieldName, const autil::StringView& value, bool needCopy);
    void AppendAccessaryField(const autil::StringView& fieldName, const autil::StringView& value, bool needCopy);
    void AppendNonExistAccessaryField(const std::string& fieldName);

    // no value copy, just reference to other
    bool Merge(const std::shared_ptr<SourceDocument>& other);

    bool operator==(const SourceDocument& right) const;
    bool operator!=(const SourceDocument& right) const;

    SourceGroupFieldIter CreateGroupFieldIter(index::sourcegroupid_t groupId) const;
    SourceMetaIter CreateGroupMetaIter(index::sourcegroupid_t groupId) const;
    size_t GetGroupCount() const;

    SourceGroupFieldIter CreateAccessaryFieldIter() const;
    SourceMetaIter CreateAccessaryMetaIter() const;

    const autil::StringView& GetField(index::sourcegroupid_t groupId, const std::string& fieldName) const;
    const autil::StringView& GetField(const std::string& fieldName) const;

    const autil::StringView& GetAccessaryField(const std::string& fieldName) const;

    // is field defined in group
    bool HasField(index::sourcegroupid_t groupId, const std::string& fieldName) const;

    // is field defined by not exist
    bool IsNonExistField(index::sourcegroupid_t groupId, const std::string& fieldName) const;

    void ToRawDocument(indexlibv2::document::RawDocument& rawDoc,
                       const std::set<std::string>& requiredFields = {}) const;

    void ExtractFields(std::vector<std::string>& fieldNames, std::vector<std::string>& fieldValues,
                       const std::set<std::string>& requiredFields = {}) const;

    autil::StringView EncodeNonExistFieldInfo(autil::mem_pool::PoolBase* pool);
    Status DecodeNonExistFieldInfo(const autil::StringView& str);

    autil::mem_pool::PoolBase* GetPool();

private:
    bool _ownPool = false;
    autil::mem_pool::PoolBase* _pool = nullptr;
    std::vector<SourceGroupPtr> _data;
    SourceGroupPtr _accessaryData;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::document
