#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateIndexNameMapper);

#define ITEM_INDEX_NAME "index_name"
#define ITEM_TRUNC_NAME "truncate_index_names"
#define TRUNCATE_INDEX_MAPPER_FILE "index.mapper"

TruncateIndexNameItem::TruncateIndexNameItem()
{
}

TruncateIndexNameItem::TruncateIndexNameItem(
        const string& indexName,
        const vector<string>& truncNames)
    : mIndexName(indexName)
{
    mTruncateNames.assign(truncNames.begin(), truncNames.end());
}

void TruncateIndexNameItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ITEM_INDEX_NAME, mIndexName);
    json.Jsonize(ITEM_TRUNC_NAME, mTruncateNames);
}

TruncateIndexNameMapper::TruncateIndexNameMapper(
        const std::string& truncateMetaDir)
    : mTruncateMetaDir(truncateMetaDir)
{
}

TruncateIndexNameMapper::~TruncateIndexNameMapper() 
{
}

void TruncateIndexNameMapper::Jsonize(JsonWrapper& json)
{
    json.Jsonize(TRUNCATE_INDEX_NAME_MAPPER, mTruncateNameItems, mTruncateNameItems);

    if (json.GetMode() == FROM_JSON) 
    {
        for (size_t i = 0; i < mTruncateNameItems.size(); ++i)
        {
            TruncateIndexNameItemPtr item = mTruncateNameItems[i];
            mNameMap[item->mIndexName] = i;
        }
    }
}

void TruncateIndexNameMapper::Load()
{
    string mapperFile = FileSystemWrapper::JoinPath(
            mTruncateMetaDir, TRUNCATE_INDEX_MAPPER_FILE);
    string content;
    if (!FileSystemWrapper::AtomicLoad(mapperFile, content, true))
    {
        IE_LOG(INFO, "%s not exist", mapperFile.c_str());
        return;
    }
    Any any = ParseJson(content);
    Jsonizable::JsonWrapper jsonWrapper(any);
    Jsonize(jsonWrapper);
}

void TruncateIndexNameMapper::Load(const file_system::DirectoryPtr& metaDir)
{
    string content;
    if (!metaDir->LoadMayNonExist(TRUNCATE_INDEX_MAPPER_FILE, content, true))
    {
        IE_LOG(INFO, "%s/%s not exist", metaDir->GetPath().c_str(), TRUNCATE_INDEX_MAPPER_FILE);
        return;
    }
    Any any = ParseJson(content);
    Jsonizable::JsonWrapper jsonWrapper(any);
    Jsonize(jsonWrapper);
}

void TruncateIndexNameMapper::Dump(const IndexSchemaPtr& indexSchema)
{
    string mapperFile = FileSystemWrapper::JoinPath(
            mTruncateMetaDir, TRUNCATE_INDEX_MAPPER_FILE);
    if (FileSystemWrapper::IsExist(mapperFile))
    {
        IE_LOG(INFO, "%s already exist", mapperFile.c_str());
        return;
    }

    assert(indexSchema);
    
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConf = *iter;
        const string& nonTruncName = indexConf->GetNonTruncateIndexName();
        const string& indexName = indexConf->GetIndexName();
        if (indexConf->GetShardingIndexConfigs().size() > 0)
        {
            // if is sharding index, don't writer origin index_name to index_mapper
            continue;
        }
        if (indexConf->IsVirtual() && !nonTruncName.empty())
        {
            // truncate index config
            TruncateIndexNameItemPtr item = Lookup(nonTruncName);
            if (item)
            {
                item->mTruncateNames.push_back(indexName);
            }
            else
            {
                vector<string> truncNames;
                truncNames.push_back(indexName);
                AddItem(nonTruncName, truncNames);
            }
        }
    }

    DoDump(mapperFile);
}

void TruncateIndexNameMapper::AddItem(
        const string &indexName, 
        const vector<string>& truncateIndexNames)
{
    TruncateIndexNameItemPtr item(new TruncateIndexNameItem(
                    indexName, truncateIndexNames));
    size_t pos = mTruncateNameItems.size();
    mTruncateNameItems.push_back(item);
    mNameMap[indexName] = pos;
}

void TruncateIndexNameMapper::DoDump(const string& mapperFile)
{
    Any any = autil::legacy::ToJson(*this);
    string str = ToString(any);
    FileSystemWrapper::AtomicStore(mapperFile, str);
}

TruncateIndexNameItemPtr TruncateIndexNameMapper::Lookup(
        const std::string& indexName) const
{
    NameMap::const_iterator it = mNameMap.find(indexName);
    if (it == mNameMap.end())
    {
        return TruncateIndexNameItemPtr();
    }
    
    size_t idx = it->second;
    assert(idx < mTruncateNameItems.size());
    return mTruncateNameItems[idx];
}

bool TruncateIndexNameMapper::Lookup(
        const std::string& indexName,
        std::vector<std::string>& truncNames) const
{
    TruncateIndexNameItemPtr item = Lookup(indexName);
    if (!item)
    {
        return false;
    }

    if (item->mIndexName != indexName)
    {
        IE_LOG(ERROR, "indexName [%s:%s] not match!", 
               indexName.c_str(), item->mIndexName.c_str());
        return false;
    }

    truncNames.assign(item->mTruncateNames.begin(), 
                      item->mTruncateNames.end());
    return true;
}

IE_NAMESPACE_END(config);

