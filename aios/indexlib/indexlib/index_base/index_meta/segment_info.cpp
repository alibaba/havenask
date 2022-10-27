#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/document/document.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentInfo);

void SegmentInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    uint64_t tempDocCount = docCount;
    json.Jsonize("DocCount", tempDocCount, tempDocCount);
    docCount = tempDocCount;
    json.Jsonize("Timestamp", timestamp, timestamp);
    json.Jsonize("MaxTTL", maxTTL, maxTTL);
    json.Jsonize("IsMergedSegment", mergedSegment, mergedSegment);
    json.Jsonize("ShardingColumnCount", shardingColumnCount, shardingColumnCount);
    json.Jsonize("ShardingColumnId", shardingColumnId, shardingColumnId);
    if (json.GetMode() == TO_JSON) {
        string locatorString = autil::StringUtil::strToHexStr(locator.ToString());
        json.Jsonize("Locator", locatorString);
        if (schemaId != DEFAULT_SCHEMAID)
        {
            json.Jsonize("schemaVersionId", schemaId);
        }
    } else { 
        string locatorString;
        json.Jsonize("Locator", locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        locator.SetLocator(locatorString);
        json.Jsonize("schemaVersionId", schemaId, schemaId);
    }
}

bool SegmentInfo::Load(const string& path)
{
    string content;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(path, content, true))
        {
            return false;
        }
    }
    catch (...)
    {
        throw;
    }
    
    FromString(content);
    return true;
}

bool SegmentInfo::Load(const file_system::DirectoryPtr& directory)
{
    string content;
    try
    {
        directory->Load(SEGMENT_INFO_FILE_NAME, content, true);
    }
    catch (const misc::NonExistException& e)
    {
        IE_LOG(ERROR, "segment info not exist in dir [%s]",
               directory->GetPath().c_str());
        return false;
    }
    catch (...)
    {
        throw;
    }
    
    FromString(content);
    return true;
}

void SegmentInfo::Store(const file_system::DirectoryPtr& directory) const
{
    string content = ToString();
    string segmentInfo = SEGMENT_INFO_FILE_NAME;
    directory->Store(segmentInfo, content, true);
}

//TODO:
void SegmentInfo::Store(const string& path) const
{
    string content = ToString();
    FileSystemWrapper::AtomicStore(path, content);
}

// TODO: parallel merge end_merge use this to store segment_info
void SegmentInfo::StoreIgnoreExist(const string& path) const
{
    string content = ToString();
    FileSystemWrapper::AtomicStoreIgnoreExist(path, content);
}

void SegmentInfo::Update(const DocumentPtr& doc)
{
    const Locator& docLocator = doc->GetLocator();
    if (docLocator.IsValid())
    {
        locator = doc->GetLocator();
    }
    if (timestamp < doc->GetTimestamp()) 
    {
        timestamp = doc->GetTimestamp();
    }
    if (maxTTL < doc->GetTTL()) {
        maxTTL = doc->GetTTL();
    }
}

bool SegmentInfo::HasMultiSharding() const
{
    return shardingColumnCount > 1 && shardingColumnId == INVALID_SHARDING_ID;
}

IE_NAMESPACE_END(index_base);

