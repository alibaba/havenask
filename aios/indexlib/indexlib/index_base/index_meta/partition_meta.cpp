#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/sort_pattern_transformer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, PartitionMeta);

PartitionMeta::PartitionMeta() 
{
}

PartitionMeta::PartitionMeta(const SortDescriptions &sortDescs) 
    : mSortDescriptions(sortDescs)
{
}

PartitionMeta::~PartitionMeta() 
{
}

void PartitionMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("PartitionMeta", mSortDescriptions, mSortDescriptions);
}

SortPattern PartitionMeta::GetSortPattern(const string& sortField)
{
    for (size_t i = 0; i < mSortDescriptions.size(); ++i) 
    {
        if (mSortDescriptions[i].sortFieldName == sortField)
        {
            return mSortDescriptions[i].sortPattern;
        }
    }
    return sp_nosort;
}

const SortDescription& PartitionMeta::GetSortDescription(size_t idx) const
{
    assert(idx < mSortDescriptions.size());
    return mSortDescriptions[idx];
}

void PartitionMeta::AddSortDescription(const string &sortField,
        const SortPattern &sortPattern)
{
    SortDescription meta;
    meta.sortFieldName = sortField;
    meta.sortPattern = sortPattern;
    mSortDescriptions.push_back(meta);
}

void PartitionMeta::AddSortDescription(const string &sortField,
                                       const string &sortPatternStr)
{
    SortPattern sp = SortPatternTransformer::FromString(sortPatternStr);
    AddSortDescription(sortField, sp);    
}

void PartitionMeta::AssertEqual(const PartitionMeta &other)
{
    if (mSortDescriptions.size() != other.mSortDescriptions.size()) 
    {
        INDEXLIB_THROW(misc::AssertEqualException, "SortDescription Count is not equal");
    }
    for (size_t i = 0; i < mSortDescriptions.size(); i++)
    {
        if (mSortDescriptions[i] != other.mSortDescriptions[i])
        {
            stringstream ss;
            ss << "%s is not equal to %s" << mSortDescriptions[i].ToString().c_str()
               << other.mSortDescriptions[i].ToString().c_str();
            INDEXLIB_THROW(misc::AssertEqualException, "%s", ss.str().c_str());
        }
    } 
}

bool PartitionMeta::HasSortField(const string& sortField) const
{
    for (size_t i = 0; i < mSortDescriptions.size(); i++)
    {
        if (mSortDescriptions[i].sortFieldName == sortField)
        {
            return true;
        }
    }
    return false;
}

void PartitionMeta::Store(
        const file_system::DirectoryPtr& directory) const
{
    string jsonStr = autil::legacy::ToJsonString(*this);

    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME))
    {
        IE_LOG(DEBUG, "old index partition meta file exist, auto delete!");
        directory->RemoveFile(INDEX_PARTITION_META_FILE_NAME);
    }

    directory->Store(INDEX_PARTITION_META_FILE_NAME, jsonStr, true);
}

void PartitionMeta::Store(const string &rootDir) const
{
    string fileName = FileSystemWrapper::JoinPath(
            rootDir, INDEX_PARTITION_META_FILE_NAME);
    string jsonStr = autil::legacy::ToJsonString(*this);
    FileSystemWrapper::AtomicStoreIgnoreExist(fileName, jsonStr);
}

void PartitionMeta::Load(const string &rootDir)
{
    string fileName = FileSystemWrapper::JoinPath(
            rootDir, INDEX_PARTITION_META_FILE_NAME);
    string jsonStr;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(fileName, jsonStr, true))
        {
            return;
        }
    }
    catch (...)
    {
        throw;
    }
    *this = LoadFromString(jsonStr);
}

PartitionMeta PartitionMeta::LoadFromString(const string& jsonStr)
{
    PartitionMeta partMeta;
    try
    {
        autil::legacy::FromJsonString(partMeta, jsonStr);
    }
    catch (const JsonParserError& e)
    {
        stringstream ss;
        ss << "Parse index partition meta file FAILED,"
           << "jsonStr: " << jsonStr 
           << "exception: " << e.what(); 
        
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "%s", ss.str().c_str());
    }
    return partMeta;
}

void PartitionMeta::Load(const file_system::DirectoryPtr& directory)
{
    string jsonStr;
    directory->Load(INDEX_PARTITION_META_FILE_NAME, jsonStr, true);
    *this = LoadFromString(jsonStr);
}

PartitionMeta PartitionMeta::LoadPartitionMeta(const std::string &rootDir) {
    PartitionMeta meta;
    meta.Load(rootDir);
    return meta;
}

bool PartitionMeta::IsExist(const string &indexRoot)
{
    string fileName = FileSystemWrapper::JoinPath(
            indexRoot, INDEX_PARTITION_META_FILE_NAME);
    return FileSystemWrapper::IsExist(fileName);
}

IE_NAMESPACE_END(index_base);

