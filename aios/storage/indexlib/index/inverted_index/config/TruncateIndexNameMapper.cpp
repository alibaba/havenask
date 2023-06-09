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
#include "indexlib/index/inverted_index/config/TruncateIndexNameMapper.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TruncateIndexNameMapper);
namespace {
static const std::string TRUNCATE_INDEX_MAPPER_FILE = "index.mapper";
}

TruncateIndexNameItem::TruncateIndexNameItem() {}

TruncateIndexNameItem::TruncateIndexNameItem(const std::string& name, const std::vector<std::string>& truncNames)
    : indexName(name)
{
    truncateNames.assign(truncNames.begin(), truncNames.end());
}

void TruncateIndexNameItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("index_name", indexName);
    json.Jsonize("truncate_index_names", truncateNames);
}

TruncateIndexNameMapper::TruncateIndexNameMapper(
    const std::shared_ptr<indexlib::file_system::IDirectory>& truncateMetaDir)
    : _truncateMetaDir(truncateMetaDir)
{
}

TruncateIndexNameMapper::~TruncateIndexNameMapper() {}

void TruncateIndexNameMapper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("truncate_index_name_mapper", _truncateNameItems, _truncateNameItems);

    if (json.GetMode() == FROM_JSON) {
        for (size_t i = 0; i < _truncateNameItems.size(); ++i) {
            auto item = _truncateNameItems[i];
            _nameMap[item->indexName] = i;
        }
    }
}

Status TruncateIndexNameMapper::Load() { return Load(_truncateMetaDir); }

Status TruncateIndexNameMapper::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& metaDir)
{
    std::string content;
    auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
    readerOption.mayNonExist = true;
    auto [status, isExist] = metaDir->IsExist(TRUNCATE_INDEX_MAPPER_FILE).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "check truncate index name mapper existence failed, dir:%s",
                           metaDir->DebugString().c_str());
    if (!isExist) {
        return Status::OK();
    }
    auto fsResult = metaDir->Load(TRUNCATE_INDEX_MAPPER_FILE, readerOption, content);
    if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_OK) {
        AUTIL_LOG(ERROR, "load truncate index name mapper failed, dir:%s", metaDir->DebugString().c_str());
        return fsResult.Status();
    }
    if (content.empty()) {
        return Status::OK();
    }
    try {
        autil::legacy::Any any = autil::legacy::json::ParseJson(content);
        Jsonizable::JsonWrapper jsonWrapper(any);
        Jsonize(jsonWrapper);
    } catch (...) {
        AUTIL_LOG(ERROR, "load truncate index name mapper failed, content[%s]", content.c_str());
        return Status::InternalError("deserialize truncate index name mapper failed.");
    }
    return Status::OK();
}

Status TruncateIndexNameMapper::Dump(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs)
{
    if (_truncateMetaDir->IsExist(TRUNCATE_INDEX_MAPPER_FILE).Value()) {
        AUTIL_LOG(INFO, "%s already exist in %s", TRUNCATE_INDEX_MAPPER_FILE.c_str(),
                  _truncateMetaDir->DebugString().c_str());
        return Status::OK();
    }
    for (auto indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig != nullptr);

        std::string indexName = invertedIndexConfig->GetIndexName();
        auto addTruncateIndexNames = [this](const std::shared_ptr<InvertedIndexConfig>& invertedConfig) {
            for (auto truncateIndexConfig : invertedConfig->GetTruncateIndexConfigs()) {
                const std::string& truncateIndexName = truncateIndexConfig->GetIndexName();
                std::shared_ptr<TruncateIndexNameItem> item = Lookup(invertedConfig->GetIndexName());
                if (item) {
                    item->truncateNames.push_back(truncateIndexName);
                } else {
                    std::vector<std::string> truncateIndexNames;
                    truncateIndexNames.emplace_back(std::move(truncateIndexName));
                    AddItem(invertedConfig->GetIndexName(), truncateIndexNames);
                }
            }
        };
        if (invertedIndexConfig->GetShardingType() ==
            config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING) {
            for (const auto& shardConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
                auto shardInvertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(shardConfig);
                assert(shardInvertedIndexConfig != nullptr);
                addTruncateIndexNames(shardInvertedIndexConfig);
            }
        } else {
            assert(invertedIndexConfig->GetShardingType() ==
                   config::InvertedIndexConfig::IndexShardingType::IST_NO_SHARDING);
            addTruncateIndexNames(invertedIndexConfig);
        }
    }
    return DoDump();
}

void TruncateIndexNameMapper::AddItem(const std::string& indexName, const std::vector<std::string>& truncateIndexNames)
{
    auto item = std::make_shared<TruncateIndexNameItem>(indexName, truncateIndexNames);
    size_t pos = _truncateNameItems.size();
    _truncateNameItems.emplace_back(std::move(item));
    _nameMap[indexName] = pos;
}

Status TruncateIndexNameMapper::DoDump()
{
    std::string str;
    auto st = indexlib::file_system::JsonUtil::ToString(*this, &str).Status();
    RETURN_IF_STATUS_ERROR(st, "serialize truncate index mapper failed.");
    // TODO(qingran): the way it generated is not consitent with lfs, for it change the read_only dir
    std::string dirPath = _truncateMetaDir->GetOutputPath();
    std::shared_ptr<indexlib::file_system::IDirectory> physicalDir =
        indexlib::file_system::IDirectory::GetPhysicalDirectory(dirPath);
    indexlib::file_system::FileSystemMetricsReporter* reporter =
        _truncateMetaDir->GetFileSystem() != nullptr ? _truncateMetaDir->GetFileSystem()->GetFileSystemMetricsReporter()
                                                     : nullptr;
    std::shared_ptr<indexlib::file_system::IDirectory> directory =
        indexlib::file_system::IDirectory::ConstructByFenceContext(physicalDir, _truncateMetaDir->GetFenceContext(),
                                                                   reporter);
    st = directory->Store(TRUNCATE_INDEX_MAPPER_FILE, str, indexlib::file_system::WriterOption::AtomicDump()).Status();
    RETURN_IF_STATUS_ERROR(st, "store truncate index mapper failed.");
    st = _truncateMetaDir->GetFileSystem()
             ->MountFile(dirPath, TRUNCATE_INDEX_MAPPER_FILE, TRUNCATE_INDEX_MAPPER_FILE,
                         indexlib::file_system::FSMT_READ_ONLY, /*length*/ -1, false)
             .Status();
    RETURN_IF_STATUS_ERROR(st, "mount truncate index mapper file failed.");
    return st;
}

std::shared_ptr<TruncateIndexNameItem> TruncateIndexNameMapper::Lookup(const std::string& indexName) const
{
    NameMap::const_iterator it = _nameMap.find(indexName);
    if (it == _nameMap.end()) {
        return nullptr;
    }

    size_t idx = it->second;
    assert(idx < _truncateNameItems.size());
    return _truncateNameItems[idx];
}

bool TruncateIndexNameMapper::Lookup(const std::string& name, std::vector<std::string>& truncNames) const
{
    std::shared_ptr<TruncateIndexNameItem> item = Lookup(name);
    if (!item) {
        return false;
    }

    if (item->indexName != name) {
        AUTIL_LOG(ERROR, "indexName [%s:%s] not match!", name.c_str(), item->indexName.c_str());
        return false;
    }

    truncNames.assign(item->truncateNames.begin(), item->truncateNames.end());
    return true;
}

} // namespace indexlibv2::config