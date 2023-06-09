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
#include "indexlib/index/aggregation/PostingMap.h"

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/aggregation/FixedSizeValueList.h"
#include "indexlib/index/aggregation/IndexMeta.h"
#include "indexlib/index/aggregation/KeyMeta.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PostingMap);

PostingMap::PostingMap(autil::mem_pool::PoolBase* objPool, autil::mem_pool::Pool* bufferPool,
                       const std::shared_ptr<config::AggregationIndexConfig>& indexConfig, size_t initialKeyCount)
    : _objPool(objPool)
    , _bufferPool(bufferPool)
    , _indexConfig(indexConfig)
{
    _keyVector = std::make_unique<KeyVector>(_bufferPool, initialKeyCount);
    _postingMap = IE_POOL_NEW_CLASS(_objPool, PostingTable, _bufferPool, initialKeyCount);
}

PostingMap::~PostingMap()
{
    if (!_postingMap) {
        return;
    }
    auto it = _postingMap->CreateIterator();
    while (it.HasNext()) {
        IValueList* valueList = it.Next().second;
        indexlib::IE_POOL_DELETE_CLASS(_objPool, valueList);
    }
    _postingMap->Clear();
    indexlib::IE_POOL_DELETE_CLASS(_objPool, _postingMap);
    _postingMap = nullptr;
}

Status PostingMap::Add(uint64_t key, const autil::StringView& data)
{
    IValueList** it = _postingMap->Find(key);
    IValueList* valueList = nullptr;
    if (it == nullptr) {
        valueList = IE_POOL_NEW_CLASS(_objPool, FixedSizeValueList, _objPool, _indexConfig);
        auto s = valueList->Init();
        if (!s.IsOK()) {
            indexlib::IE_POOL_DELETE_CLASS(_objPool, valueList);
            return s;
        }
        _postingMap->Insert(key, valueList);
        _keyVector->Append(key);
    } else {
        valueList = *it;
    }
    auto s = valueList->Append(data);
    if (!s.IsOK()) {
        return s;
    }
    _totalBytes += data.size();
    _maxValueCount = std::max(_maxValueCount, valueList->GetTotalCount());
    _maxValueBytes = std::max(_maxValueBytes, valueList->GetTotalBytes());
    return Status::OK();
}

Status PostingMap::Dump(autil::mem_pool::PoolBase* dumpPool,
                        const std::shared_ptr<indexlib::file_system::IDirectory>& indexDir)
{
    auto [s, fileWriter] = CreateFileWriter(indexDir, VALUE_FILE_NAME);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "make index directory %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }

    // 1. sort key
    using Allocator = autil::mem_pool::pool_allocator<KeyMeta>;
    using KeyMetaVector = std::vector<KeyMeta, Allocator>;
    KeyMetaVector keyMetas(_objPool);
    keyMetas.reserve(_keyVector->GetKeyCount());
    for (auto it = _keyVector->CreateIterator(); it->HasNext();) {
        KeyMeta meta;
        meta.key = it->Next();
        keyMetas.emplace_back(std::move(meta));
    }
    std::sort(keyMetas.begin(), keyMetas.end());

    // 2. for key in keys: sort && dump value list
    IndexMeta indexMeta;
    for (auto& keyMeta : keyMetas) {
        IValueList** it = _postingMap->Find(keyMeta.key);
        assert(it != nullptr && *it != nullptr);
        IValueList* list = *it;
        keyMeta.offset = fileWriter->GetLogicLength();
        auto r = list->Dump(fileWriter, dumpPool);
        if (!r.IsOK()) {
            return {r.steal_error()};
        }
        auto stat = std::move(r.steal_value());
        keyMeta.valueMeta.valueCount = stat.valueCount;
        keyMeta.valueMeta.valueCountAfterUnique = stat.valueCountAfterUnique;
        indexMeta.Update(stat);
    }
    s = fileWriter->Close().Status();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "close value file for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }

    // 3. dump keys
    std::tie(s, fileWriter) = CreateFileWriter(indexDir, KEY_FILE_NAME);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "create key file for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }
    const char* data = reinterpret_cast<const char*>(keyMetas.data());
    s = fileWriter->Write(data, keyMetas.size() * sizeof(KeyMeta)).Status();
    if (s.IsOK()) {
        s = fileWriter->Close().Status();
    }
    if (!s.IsOK()) {
        return s;
    }

    // 4. dump index meta
    std::tie(s, fileWriter) = CreateFileWriter(indexDir, META_FILE_NAME);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "create meta file for %s failed, error: %s", _indexConfig->GetIndexName().c_str(),
                  s.ToString().c_str());
        return s;
    }
    auto str = ToJsonString(indexMeta);
    s = fileWriter->Write(str.c_str(), str.size()).Status();
    if (s.IsOK()) {
        s = fileWriter->Close().Status();
    }
    return s;
}

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
PostingMap::CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                             const std::string& fileName) const
{
    indexlib::file_system::WriterOption opts;
    opts.openType = indexlib::file_system::FSOT_BUFFERED;
    return directory->CreateFileWriter(fileName, opts).StatusWith();
}

const IValueList* PostingMap::Lookup(uint64_t key) const
{
    IValueList** it = _postingMap->Find(key);
    if (it == nullptr) {
        return nullptr;
    }
    return *it;
}

} // namespace indexlibv2::index
