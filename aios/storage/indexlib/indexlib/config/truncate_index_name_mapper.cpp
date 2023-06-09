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
#include "indexlib/config/truncate_index_name_mapper.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::file_system;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, TruncateIndexNameMapper);

#define ITEM_INDEX_NAME "index_name"
#define ITEM_TRUNC_NAME "truncate_index_names"
#define TRUNCATE_INDEX_MAPPER_FILE "index.mapper"

TruncateIndexNameItem::TruncateIndexNameItem() {}

TruncateIndexNameItem::TruncateIndexNameItem(const string& indexName, const vector<string>& truncNames)
    : mIndexName(indexName)
{
    mTruncateNames.assign(truncNames.begin(), truncNames.end());
}

void TruncateIndexNameItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ITEM_INDEX_NAME, mIndexName);
    json.Jsonize(ITEM_TRUNC_NAME, mTruncateNames);
}

TruncateIndexNameMapper::TruncateIndexNameMapper(const file_system::DirectoryPtr& truncateMetaDir)
    : mTruncateMetaDir(truncateMetaDir)
{
}

TruncateIndexNameMapper::~TruncateIndexNameMapper() {}

void TruncateIndexNameMapper::Jsonize(JsonWrapper& json)
{
    json.Jsonize(TRUNCATE_INDEX_NAME_MAPPER, mTruncateNameItems, mTruncateNameItems);

    if (json.GetMode() == FROM_JSON) {
        for (size_t i = 0; i < mTruncateNameItems.size(); ++i) {
            TruncateIndexNameItemPtr item = mTruncateNameItems[i];
            mNameMap[item->mIndexName] = i;
        }
    }
}

void TruncateIndexNameMapper::Load() { Load(mTruncateMetaDir); }

void TruncateIndexNameMapper::Load(const file_system::DirectoryPtr& metaDir)
{
    string content;
    if (!metaDir->LoadMayNonExist(TRUNCATE_INDEX_MAPPER_FILE, content, true)) {
        IE_LOG(INFO, "%s/%s not exist", metaDir->DebugString().c_str(), TRUNCATE_INDEX_MAPPER_FILE);
        return;
    }
    Any any = ParseJson(content);
    Jsonizable::JsonWrapper jsonWrapper(any);
    Jsonize(jsonWrapper);
}

void TruncateIndexNameMapper::Dump(const IndexSchemaPtr& indexSchema)
{
    if (mTruncateMetaDir->IsExist(TRUNCATE_INDEX_MAPPER_FILE)) {
        IE_LOG(INFO, "%s already exist in %s", TRUNCATE_INDEX_MAPPER_FILE, mTruncateMetaDir->DebugString().c_str());
        return;
    }
    assert(indexSchema);

    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConf = *iter;
        const string& nonTruncName = indexConf->GetNonTruncateIndexName();
        const string& indexName = indexConf->GetIndexName();
        if (indexConf->GetShardingIndexConfigs().size() > 0) {
            // if is sharding index, don't writer origin index_name to index_mapper
            continue;
        }
        if (indexConf->IsVirtual() && !nonTruncName.empty()) {
            // truncate index config
            TruncateIndexNameItemPtr item = Lookup(nonTruncName);
            if (item) {
                item->mTruncateNames.push_back(indexName);
            } else {
                vector<string> truncNames;
                truncNames.push_back(indexName);
                AddItem(nonTruncName, truncNames);
            }
        }
    }
    DoDump();
}

void TruncateIndexNameMapper::AddItem(const string& indexName, const vector<string>& truncateIndexNames)
{
    TruncateIndexNameItemPtr item(new TruncateIndexNameItem(indexName, truncateIndexNames));
    size_t pos = mTruncateNameItems.size();
    mTruncateNameItems.push_back(item);
    mNameMap[indexName] = pos;
}

void TruncateIndexNameMapper::DoDump()
{
    Any any = autil::legacy::ToJson(*this);
    string str = ToString(any);
    // TODO(qingran): the way it generated is not consitent with lfs, for it change the read_only dir
    std::string dirPath = mTruncateMetaDir->GetOutputPath();
    file_system::DirectoryPtr physicalDir = file_system::Directory::GetPhysicalDirectory(dirPath);
    FileSystemMetricsReporter* reporter = mTruncateMetaDir->GetFileSystem() != nullptr
                                              ? mTruncateMetaDir->GetFileSystem()->GetFileSystemMetricsReporter()
                                              : nullptr;
    file_system::DirectoryPtr directory =
        file_system::Directory::ConstructByFenceContext(physicalDir, mTruncateMetaDir->GetFenceContext(), reporter);
    directory->Store(TRUNCATE_INDEX_MAPPER_FILE, str, WriterOption::AtomicDump());
    THROW_IF_FS_ERROR(mTruncateMetaDir->GetFileSystem()->MountFile(dirPath, TRUNCATE_INDEX_MAPPER_FILE,
                                                                   TRUNCATE_INDEX_MAPPER_FILE,
                                                                   file_system::FSMT_READ_ONLY, -1, false),
                      "mount truncate index mapper file failed");
}

TruncateIndexNameItemPtr TruncateIndexNameMapper::Lookup(const std::string& indexName) const
{
    NameMap::const_iterator it = mNameMap.find(indexName);
    if (it == mNameMap.end()) {
        return TruncateIndexNameItemPtr();
    }

    size_t idx = it->second;
    assert(idx < mTruncateNameItems.size());
    return mTruncateNameItems[idx];
}

bool TruncateIndexNameMapper::Lookup(const std::string& indexName, std::vector<std::string>& truncNames) const
{
    TruncateIndexNameItemPtr item = Lookup(indexName);
    if (!item) {
        return false;
    }

    if (item->mIndexName != indexName) {
        IE_LOG(ERROR, "indexName [%s:%s] not match!", indexName.c_str(), item->mIndexName.c_str());
        return false;
    }

    truncNames.assign(item->mTruncateNames.begin(), item->mTruncateNames.end());
    return true;
}
}} // namespace indexlib::config
