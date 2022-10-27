#include "indexlib/config/impl/index_schema_impl.h"
#include <sstream>
#include "indexlib/misc/exception.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/json.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/truncate_index_name_mapper.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std::tr1;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, IndexSchemaImpl);

IndexConfigPtr IndexSchemaImpl::GetIndexConfig(const string& indexName) const 
{
    NameMap::const_iterator it = mIndexName2IdMap.find(indexName);
    IndexConfigPtr ret;
    if (it != mIndexName2IdMap.end())
    {
        ret = mIndexConfigs[it->second];
    }
    else 
    {
        it = mVirtualIndexname2IdMap.find(indexName);
        if (it != mVirtualIndexname2IdMap.end())
        {
            ret = mVirtualIndexConfigs[it->second - mIndexConfigs.size()];
        }
    }

    if (ret && !ret->IsDeleted())
    {
        return ret;
    }
    return IndexConfigPtr();
}

void IndexSchemaImpl::Check()
{
    CheckTireIndex();
    for (size_t i = 0; i < mIndexConfigs.size(); i++)
    {
        IndexConfigPtr indexConfig = mIndexConfigs[i];
        indexConfig->Check();
        CheckIndexNameNotSummary(indexConfig);
    }
}

void IndexSchemaImpl::CheckIndexNameNotSummary(const IndexConfigPtr& indexConfig) const
{
    string indexName = indexConfig->GetIndexName();
    if (indexName == "summary")
    {
        stringstream ss;
        ss << "One index's name is \"summary\", it is forbidden." << endl;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
}

void IndexSchemaImpl::CheckTireIndex()
{
    if (!mPrimaryKeyIndexConfig)
    {
        return;
    }

    if (mPrimaryKeyIndexConfig->GetIndexType() == it_trie)
    {
        if (mIndexConfigs.size() > 1)
        {
            INDEXLIB_FATAL_ERROR(Schema, "trie index not support other indexes");
        }
    }
}
IndexConfigPtr IndexSchemaImpl::GetIndexConfig(indexid_t id) const 
{
    if (id == INVALID_INDEXID)
    {
        return IndexConfigPtr();
    }
    indexid_t firstVirtualIndexId = (indexid_t)mIndexConfigs.size();
    indexid_t maxIndexId = firstVirtualIndexId + 
                           (indexid_t)mVirtualIndexConfigs.size();

    IndexConfigPtr ret;    
    if (id < firstVirtualIndexId)
    {
        ret = mIndexConfigs[id];
    }
    else if (firstVirtualIndexId <= id && id < maxIndexId)
    {
        ret = mVirtualIndexConfigs[id - firstVirtualIndexId];
    }

    if (ret && !ret->IsDeleted())
    {
        return ret;
    }
    return IndexConfigPtr();
}

bool IndexSchemaImpl::IsDeleted(indexid_t id) const
{
    if (id == INVALID_INDEXID) {
        return false;
    }
    indexid_t firstVirtualIndexId = (indexid_t)mIndexConfigs.size();
    indexid_t maxIndexId = firstVirtualIndexId + 
                           (indexid_t)mVirtualIndexConfigs.size();

    IndexConfigPtr ret;    
    if (id < firstVirtualIndexId)
    {
        ret = mIndexConfigs[id];
    }
    else if (firstVirtualIndexId <= id && id < maxIndexId)
    {
        ret = mVirtualIndexConfigs[id - firstVirtualIndexId];
    }
    return ret && ret->IsDeleted();
}

void IndexSchemaImpl::AddIndexConfig(const IndexConfigPtr& indexConfig)
{
    assert(indexConfig);

    if (!indexConfig->IsVirtual())
    {
        if (mOnlyAddVirtual)
        {
            INDEXLIB_FATAL_ERROR(UnSupported, "add index [%s] fail, "
                    "only support add virtual index",
                    indexConfig->GetIndexName().c_str());
        }
        AddIndexConfig(indexConfig, mIndexName2IdMap, mIndexConfigs);
        ResetVirtualIndexId();
    }
    else
    {
        AddIndexConfig(indexConfig, mVirtualIndexname2IdMap, mVirtualIndexConfigs);
    }
    SetExistFlag(indexConfig);
}

void IndexSchemaImpl::AddIndexConfig(const IndexConfigPtr& indexConfig, 
                                     NameMap& nameMap,
                                     IndexConfigVector& indexConfigs)
{
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        const vector<IndexConfigPtr>& shardingIndexConfigs = 
            indexConfig->GetShardingIndexConfigs();
        for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
        {
            AddIndexConfig(shardingIndexConfigs[i], nameMap, indexConfigs);
        }
    }

    NameMap::iterator it = nameMap.find(indexConfig->GetIndexName());
    if (it != nameMap.end())
    {
        stringstream msg;
        msg << "Duplicated index: name:" << indexConfig->GetIndexName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }

    indexid_t indexId = mIndexConfigs.size();
    if (indexConfig->IsVirtual())
    {
        indexId += mVirtualIndexConfigs.size();
    }

    indexConfig->SetIndexId(indexId);
    indexConfigs.push_back(indexConfig);
    nameMap.insert(make_pair(indexConfig->GetIndexName(), 
                             indexConfig->GetIndexId()));
    IndexType type = indexConfig->GetIndexType();
    if(type == it_primarykey64 || type == it_primarykey128 || type == it_trie
       || type == it_kv || type == it_kkv)
    {
        if (mPrimaryKeyIndexConfig)
        {
            stringstream msg;
            msg << "Duplicated primarykey index, name1 = " 
                << mPrimaryKeyIndexConfig->GetIndexName()
                << ", name2 = " << indexConfig->GetIndexName();
            INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
        }
        SingleFieldIndexConfigPtr singleFieldConfig = 
            DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, indexConfig);
        assert(singleFieldConfig);
        mPrimaryKeyIndexConfig = singleFieldConfig;
        mPrimaryKeyIndexId = singleFieldConfig->GetFieldConfig()->GetFieldId();
    }

}

void IndexSchemaImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) 
    {
        int32_t count = 0;
        // json.Jsonize(INDEXS, mIndexConfigs);
        vector<Any> anyVec;
        int32_t indexCount = (mBaseIndexCount >= 0) ?
                             mBaseIndexCount : (int32_t)mIndexConfigs.size();

        IndexConfigVector::iterator it = mIndexConfigs.begin();
        for (; it != mIndexConfigs.end(); ++it)
        {
            if (count >= indexCount)
            {
                break;
            }
            ++count;
            
            if ((*it)->GetShardingType() == IndexConfig::IST_IS_SHARDING)
            {
                continue;
            }

            if (it_pack == (*it)->GetIndexType() 
                || it_expack == (*it)->GetIndexType()
                || it_customized == (*it)->GetIndexType()) 
            {
                anyVec.push_back(
                        ToJson(*(dynamic_pointer_cast<PackageIndexConfig>(*it))));
            }
            else 
            {
                anyVec.push_back(
                        ToJson(*(dynamic_pointer_cast<SingleFieldIndexConfig>(*it))));
            }
        }
        json.Jsonize(INDEXS, anyVec);
    }
}

void IndexSchemaImpl::AssertEqual(const IndexSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mIndexName2IdMap, other.mIndexName2IdMap, 
                           "mIndexName2IdMap not equal");
    IE_CONFIG_ASSERT_EQUAL(mIndexConfigs.size(), other.mIndexConfigs.size(), 
                           "mIndexConfigs's size not equal");
    for (size_t i = 0; i < mIndexConfigs.size(); ++i)
    {
        mIndexConfigs[i]->AssertEqual(*(other.mIndexConfigs[i]));
    }
}

void IndexSchemaImpl::AssertCompatible(const IndexSchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mIndexName2IdMap.size() <= other.mIndexName2IdMap.size(), 
                     "mIndexName2IdMap's size not compatible");
    IE_CONFIG_ASSERT(mIndexConfigs.size() <= other.mIndexConfigs.size(), 
                     "mIndexConfigs' size not compatible");
    for (size_t i = 0; i < mIndexConfigs.size(); ++i)
    {
        mIndexConfigs[i]->AssertCompatible(*(other.mIndexConfigs[i]));
    }
}

bool IndexSchemaImpl::IsInIndex(fieldid_t fieldId) const
{
    if ((size_t)fieldId >= mFlagVector.size() || fieldId == INVALID_FIELDID)
    {
        return false;
    }
    return !mFlagVector[fieldId].empty();
}

void IndexSchemaImpl::SetExistFlag(const IndexConfigPtr& indexConfig)
{
    assert(indexConfig);
    if (indexConfig->GetIndexType() == it_pack
        || indexConfig->GetIndexType() == it_expack
        || indexConfig->GetIndexType() == it_customized)
    {
        PackageIndexConfigPtr packageConfig = 
            dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        assert(packageConfig);
                
        const FieldConfigVector& fieldVector = packageConfig->GetFieldConfigVector();
        for (size_t i = 0; i < fieldVector.size(); ++i)
        {
            SetExistFlag(fieldVector[i]->GetFieldId(),
                         indexConfig->GetIndexId());
        }
    }
    else
    {
        SingleFieldIndexConfigPtr singleFieldConfig = 
            dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
        SetExistFlag(singleFieldConfig->GetFieldConfig()->GetFieldId(),
                     indexConfig->GetIndexId());
    }
}

void IndexSchemaImpl::SetExistFlag(fieldid_t fieldId, indexid_t indexId)
{
    assert(fieldId != INVALID_FIELDID);
    if ((size_t)fieldId >= mFlagVector.size())
    {
        mFlagVector.resize(fieldId + 1);
    }
    mFlagVector[fieldId].push_back(indexId);
}


void IndexSchemaImpl::SetNonExistFlag(const IndexConfigPtr& indexConfig)
{
    assert(indexConfig);
    if (indexConfig->GetIndexType() == it_pack
        || indexConfig->GetIndexType() == it_expack
        || indexConfig->GetIndexType() == it_customized)
    {
        PackageIndexConfigPtr packageConfig = 
            dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        assert(packageConfig);
                
        const FieldConfigVector& fieldVector = packageConfig->GetFieldConfigVector();
        for (size_t i = 0; i < fieldVector.size(); ++i)
        {
            SetNonExistFlag(fieldVector[i]->GetFieldId(),
                         indexConfig->GetIndexId());
        }
    }
    else
    {
        SingleFieldIndexConfigPtr singleFieldConfig = 
            dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
        SetNonExistFlag(singleFieldConfig->GetFieldConfig()->GetFieldId(),
                        indexConfig->GetIndexId());
    }
}

void IndexSchemaImpl::SetNonExistFlag(fieldid_t fieldId, indexid_t indexId)
{
    assert(fieldId != INVALID_FIELDID);
    if ((size_t)fieldId >= mFlagVector.size())
    {
        IE_LOG(ERROR, "fieldId [%d] not exist!", fieldId);
        return;
    }

    std::vector<indexid_t>& idVec = mFlagVector[fieldId];
    auto it = remove_if(idVec.begin(), idVec.end(),
                        [&indexId](indexid_t id) { return indexId == id; });
    idVec.erase(it, idVec.end());
}

void IndexSchemaImpl::LoadTruncateTermInfo(const string& metaDir)
{
    if (!FileSystemWrapper::IsExist(metaDir))
    {
        return;
    }
    TruncateIndexNameMapper truncMapper(metaDir);
    ArchiveFolderPtr folder(new ArchiveFolder(false));
    folder->Open(metaDir);
    truncMapper.Load();
    Iterator iter = Begin();
    while(iter != End())
    {
        IndexConfigPtr indexConfig = *iter;
        const string& indexName = indexConfig->GetIndexName();
        vector<string> truncNames;
        if (truncMapper.Lookup(indexName, truncNames))
        {
            indexConfig->LoadTruncateTermVocabulary(folder, truncNames);
        }
        iter++;
    }
    folder->Close();
}

void IndexSchemaImpl::LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir)
{
    TruncateIndexNameMapper truncMapper("");
    truncMapper.Load(metaDir);
    ArchiveFolderPtr folder = metaDir->GetArchiveFolder();
    Iterator iter = Begin();
    while(iter != End())
    {
        IndexConfigPtr indexConfig = *iter;
        const string& indexName = indexConfig->GetIndexName();
        vector<string> truncNames;
        if (truncMapper.Lookup(indexName, truncNames))
        {
            indexConfig->LoadTruncateTermVocabulary(folder, truncNames);
        }
        iter++;
    }
    folder->Close();
}

const std::vector<indexid_t>& IndexSchemaImpl::GetIndexIdList(fieldid_t fieldId) const
{
    static std::vector<indexid_t> emptyIndexIds;
    if (!IsInIndex(fieldId))
    {
        return emptyIndexIds;
    }
    assert((size_t)fieldId < mFlagVector.size());
    return mFlagVector[fieldId];
}

std::string IndexSchemaImpl::GetPrimaryKeyIndexFieldName() const
{
    if (!mPrimaryKeyIndexConfig)
    {
        return "";
    }
    return mPrimaryKeyIndexConfig->GetFieldConfig()->GetFieldName();
}

bool IndexSchemaImpl::HasPrimaryKeyAttribute() const
{
    if (!mPrimaryKeyIndexConfig)
    {
        return false;
    }
    return mPrimaryKeyIndexConfig->HasPrimaryKeyAttribute();
}

void IndexSchemaImpl::DeleteIndex(const string& indexName)
{
    IndexConfigPtr indexConfig = GetIndexConfig(indexName);
    if (!indexConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "delete index [%s] fail, index not exist!", indexName.c_str());
    }

    IndexType type = indexConfig->GetIndexType();
    if(type == it_primarykey64 || type == it_primarykey128 || type == it_trie
       || type == it_kv || type == it_kkv)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "delete index [%s] fail, not support for primaryKey/kv/kkv/trie!", indexName.c_str());
    }

    if (indexConfig->HasTruncate())
    {
        // TODO : add case for delete truncate
        vector<string> truncIndexName;
        IndexConfigIteratorPtr iter = CreateIterator(true, CIT_NOT_DELETE);
        for (auto it = iter->Begin(); it != iter->End(); it++)
        {
            if ((*it)->GetNonTruncateIndexName() == indexConfig->GetIndexName())
            {
                truncIndexName.push_back((*it)->GetIndexName());
            }
        }
        for (auto &truncName : truncIndexName)
        {
            DeleteIndex(truncName);
        }
    }

    indexConfig->Delete();
    mIndexName2IdMap.erase(indexName);
    SetNonExistFlag(indexConfig);
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        const vector<IndexConfigPtr>& shardingIndexConfigs = 
            indexConfig->GetShardingIndexConfigs();
        for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
        {
            DeleteIndex(shardingIndexConfigs[i]->GetIndexName());
        }
    }
}

void IndexSchemaImpl::DisableIndex(const string& indexName)
{
    IndexConfigPtr indexConfig = GetIndexConfig(indexName);
    if (!indexConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "disable index [%s] fail, index not exist!", indexName.c_str());
    }
    IndexType type = indexConfig->GetIndexType();
    if(type == it_primarykey64 || type == it_primarykey128 || type == it_trie
       || type == it_kv || type == it_kkv)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "disable index [%s] fail, not support for primaryKey/kv/kkv/trie!", indexName.c_str());
    }

    //TODO support the following index?
    if (indexConfig->HasTruncate())
    {
        vector<string> truncIndexName;
        IndexConfigIteratorPtr iter = CreateIterator(true, CIT_NOT_DELETE);
        for (auto it = iter->Begin(); it != iter->End(); it++)
        {
            if ((*it)->GetNonTruncateIndexName() == indexConfig->GetIndexName())
            {
                truncIndexName.push_back((*it)->GetIndexName());
            }
        }
        for (auto &truncName : truncIndexName)
        {
            DisableIndex(truncName);
        }
    }
    
    indexConfig->Disable();
    if (indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        const vector<IndexConfigPtr>& shardingIndexConfigs = 
            indexConfig->GetShardingIndexConfigs();
        for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
        {
            DisableIndex(shardingIndexConfigs[i]->GetIndexName());
        }
    }
}

void IndexSchemaImpl::SetBaseSchemaImmutable()
{
    if (mBaseIndexCount < 0)
    {
        mBaseIndexCount = (int32_t)mIndexConfigs.size();
    }
}

void IndexSchemaImpl::SetModifySchemaImmutable()
{
    if (mBaseIndexCount < 0)
    {
        mBaseIndexCount = (int32_t)mIndexConfigs.size();
    }
    mOnlyAddVirtual = true;
}

void IndexSchemaImpl::SetModifySchemaMutable()
{
    mOnlyAddVirtual = false;
}

IndexConfigIteratorPtr IndexSchemaImpl::CreateIterator(
    bool needVirtual, ConfigIteratorType type) const
{
    vector<IndexConfigPtr> ret;
    for (auto &index : mIndexConfigs)
    {
        assert(index);
        if ((index->GetStatus() & type) > 0)
        {
            ret.push_back(index);
        }
    }
    for (auto &index : mVirtualIndexConfigs)
    {
        assert(index);
        if ((index->GetStatus() & type) > 0)
        {
            ret.push_back(index);
        }
    }
    IndexConfigIteratorPtr iter(new IndexConfigIterator(ret));
    return iter;
}

void IndexSchemaImpl::CollectBaseVersionIndexInfo(
        ModifyItemVector& indexs)
{
    int32_t count = 0;
    int32_t indexCount = (mBaseIndexCount >= 0) ?
                         mBaseIndexCount : (int32_t)mIndexConfigs.size();
    IndexConfigVector::iterator it = mIndexConfigs.begin();
    for (; it != mIndexConfigs.end(); ++it)
    {
        if (count >= indexCount)
        {
            break;
        }
        ++count;

        if ((*it)->IsDeleted())
        {
            continue;
        }
        if ((*it)->GetShardingType() == IndexConfig::IST_IS_SHARDING)
        {
            continue;
        }

        ModifyItemPtr item(new ModifyItem(
                        (*it)->GetIndexName(),
                        IndexConfig::IndexTypeToStr((*it)->GetIndexType()),
                        INVALID_SCHEMA_OP_ID));
        indexs.push_back(item);
    }
}

void IndexSchemaImpl::ResetVirtualIndexId()
{
    // reset already exist virtual indexId when add normal index
    // for add index config in modify operations when truncate index exists
    for (size_t i = 0; i < mVirtualIndexConfigs.size(); i++) {
        const IndexConfigPtr& indexConfig = mVirtualIndexConfigs[i];
        const string& indexName = indexConfig->GetIndexName();
        SetNonExistFlag(indexConfig);

        indexid_t newIndexId = (indexid_t)(mIndexConfigs.size() + i);
        indexConfig->SetIndexId(newIndexId);
        SetExistFlag(indexConfig);
        mVirtualIndexname2IdMap[indexName] = newIndexId;        
    }
}

IE_NAMESPACE_END(config);

