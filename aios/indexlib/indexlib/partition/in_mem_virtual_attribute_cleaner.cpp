#include "indexlib/partition/in_mem_virtual_attribute_cleaner.h"
#include "indexlib/partition/unused_segment_collector.h"
#include "indexlib/index_define.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/partition/virtual_attribute_container.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemVirtualAttributeCleaner);

InMemVirtualAttributeCleaner::InMemVirtualAttributeCleaner(
        const ReaderContainerPtr& readerContainer,
        const VirtualAttributeContainerPtr& attrContainer,
        const DirectoryPtr& directory)
    : mReaderContainer(readerContainer)
    , mVirtualAttrContainer(attrContainer)
    , mDirectory(directory)
    , mLocalOnly(true)
{
}

InMemVirtualAttributeCleaner::~InMemVirtualAttributeCleaner() 
{
}

void InMemVirtualAttributeCleaner::ClearAttributeSegmentFile(
        const FileList& segments,
        const vector<pair<string,bool>>& attributeNames,
        const DirectoryPtr& directory)
{
    for (size_t i = 0; i < segments.size(); i++)
    {
        string segmentPath = segments[i];
        string subSegmentPath = PathUtil::JoinPath(segments[i], SUB_SEGMENT_DIR_NAME);
        string attrDirPath = PathUtil::JoinPath(segmentPath, ATTRIBUTE_DIR_NAME);
        string subAttrDirPath = PathUtil::JoinPath(subSegmentPath, ATTRIBUTE_DIR_NAME);
        for (size_t j = 0; j < attributeNames.size(); j ++)
        {
            string virAttrDirPath = PathUtil::JoinPath(
                    attributeNames[j].second ? subAttrDirPath : attrDirPath, attributeNames[j].first);
            if (directory->IsExist(virAttrDirPath))
            {
                directory->RemoveDirectory(virAttrDirPath);
            }
        }
    }    
}

void InMemVirtualAttributeCleaner::Execute()
{
    FileList segments;
    UnusedSegmentCollector::Collect(mReaderContainer, mDirectory, segments, mLocalOnly);
    const vector<pair<string,bool>>& usingVirtualAttrs = mVirtualAttrContainer->GetUsingAttributes();
    const vector<pair<string,bool>>& unusingAttrs = mVirtualAttrContainer->GetUnusingAttributes();
    vector<pair<string,bool>> restAttrs, expiredAttrs;
    for (size_t i = 0; i < unusingAttrs.size(); i ++)
    {
        if (mReaderContainer->HasAttributeReader(unusingAttrs[i].first,
                                                 unusingAttrs[i].second))
        {
            restAttrs.push_back(unusingAttrs[i]);
        }
        else
        {
            expiredAttrs.push_back(unusingAttrs[i]);
        }
    }
    
    ClearAttributeSegmentFile(segments, usingVirtualAttrs, mDirectory);
    ClearAttributeSegmentFile(segments, restAttrs, mDirectory);
    if (expiredAttrs.size() > 0)
    {
        FileList onDiskSegments, inMemSegments;
        DirectoryPtr inMemDirectory = mDirectory->GetDirectory(
                RT_INDEX_PARTITION_DIR_NAME, false);
        index_base::VersionLoader::ListSegments(mDirectory, onDiskSegments, mLocalOnly);
        index_base::VersionLoader::ListSegments(inMemDirectory, inMemSegments, mLocalOnly);
        ClearAttributeSegmentFile(onDiskSegments, expiredAttrs, mDirectory);
        ClearAttributeSegmentFile(inMemSegments, expiredAttrs, inMemDirectory);
    }
    mVirtualAttrContainer->UpdateUnusingAttributes(restAttrs);
}

IE_NAMESPACE_END(partition);

