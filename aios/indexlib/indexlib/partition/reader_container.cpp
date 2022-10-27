#include <autil/TimeUtility.h>
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReaderContainer);

ReaderContainer::ReaderContainer() 
{
}

ReaderContainer::~ReaderContainer() 
{
}

void ReaderContainer::AddReader(const IndexPartitionReaderPtr& reader)
{
    ScopedLock lock(mReaderVecLock);
    Version version = reader->GetVersion();
    mReaderVec.push_back(make_pair(version, reader));
    ResetSwitchRtSegRange();
    IE_LOG(INFO, "add reader: version[%d]", version.GetVersionId());
}

bool ReaderContainer::HasReader(const Version& version) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        if (mReaderVec[i].first == version)
        {
            return true;
        }
    }
    return false;
}

bool ReaderContainer::HasReader(versionid_t versionId) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        if (mReaderVec[i].first.GetVersionId() == versionId)
        {
            return true;
        }
    }
    return false;
}

void ReaderContainer::GetIncVersions(vector<Version>& versions) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        versions.push_back(mReaderVec[i].second->GetOnDiskVersion());
    }
}

bool ReaderContainer::IsUsingSegment(segmentid_t segmentId) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        if (mReaderVec[i].second->IsUsingSegment(segmentId))
        {
            return true;
        }
    }
    return false;
}

IndexPartitionReaderPtr ReaderContainer::GetOldestReader() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty())
    {
        return (*mReaderVec.begin()).second;
    }
    return IndexPartitionReaderPtr();
}

IndexPartitionReaderPtr ReaderContainer::GetLatestReader(versionid_t versionId) const
{
    ScopedLock lock(mReaderVecLock);
    int idx = (int)mReaderVec.size() - 1;
    for (; idx >= 0; idx --)
    {
        if (mReaderVec[idx].second->GetIncVersionId() == versionId)
        {
            return mReaderVec[idx].second;
        }
    }
    return IndexPartitionReaderPtr();
}

IndexPartitionReaderPtr ReaderContainer::PopOldestReader()
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty())
    {
        IndexPartitionReaderPtr oldestReader = (*mReaderVec.begin()).second;
        mReaderVec.erase(mReaderVec.begin());
        return oldestReader;
    }
    return IndexPartitionReaderPtr();
}

versionid_t ReaderContainer::GetLatestReaderVersion() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty())
    {
        return mReaderVec.back().second->GetVersion().GetVersionId();
    }
    return INVALID_VERSION;
}

versionid_t ReaderContainer::GetOldestReaderVersion() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty())
    {
        return (*mReaderVec.begin()).second->GetVersion().GetVersionId();
    }
    return INVALID_VERSION;
}

void ReaderContainer::EvictOldestReader()
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty())
    {
        mReaderVec.erase(mReaderVec.begin());
        ResetSwitchRtSegRange();
    }
}

bool ReaderContainer::EvictOldReaders() {
    ScopedLock lock(mReaderVecLock);
    bool doSomething = false;
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        auto& reader = mReaderVec[i].second;
        if (reader.use_count() == 1)
        {
            auto schemaName = reader->GetSchema()->GetSchemaName();
            versionid_t versionId = reader->GetVersion().GetVersionId();
            IE_LOG(INFO, "begin clean reader [%s][%d]", schemaName.c_str(), versionId);
            reader.reset();
            IE_LOG(INFO, "finish clean reader [%s][%d]", schemaName.c_str(), versionId);
            doSomething = true;
        }
        else
        {
            break;
        }
    }
    if (doSomething)
    {
        ReaderVector tmp;
        for (auto& item : mReaderVec)
        {
            if (item.second)
            {
                tmp.push_back(item);
            }
        }
        mReaderVec = std::move(tmp);
        ResetSwitchRtSegRange();
    }
    return doSomething;
}

size_t ReaderContainer::Size() const
{
     ScopedLock lock(mReaderVecLock);
     return mReaderVec.size();
}

void ReaderContainer::Close()
{
    ScopedLock lock(mReaderVecLock);
    mReaderVec.clear();
    mSwitchRtSegments.clear();
}

void ReaderContainer::ResetSwitchRtSegRange()
{
    set<segmentid_t> segIds;
    for (size_t i = 0; i < mReaderVec.size(); i++)
    {
        OnlinePartitionReaderPtr onlineReader =
            DYNAMIC_POINTER_CAST(OnlinePartitionReader, mReaderVec[i].second);
        if (!onlineReader)
        {
            continue;
        }
        const vector<segmentid_t>& switchRtSegIds = onlineReader->GetSwitchRtSegments();
        if (switchRtSegIds.empty())
        {
            continue;
        }
        segIds.insert(switchRtSegIds.begin(), switchRtSegIds.end());
    }
    mSwitchRtSegments.assign(segIds.begin(), segIds.end());
}

void ReaderContainer::GetSwitchRtSegments(std::vector<segmentid_t>& segIds) const
{
    ScopedLock lock(mReaderVecLock);
    segIds.assign(mSwitchRtSegments.begin(), mSwitchRtSegments.end());
}

bool ReaderContainer::HasAttributeReader(const std::string& attrName, bool isSub) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i)
    {
        if (isSub)
        {
            if (mReaderVec[i].second->GetSubPartitionReader()->GetAttributeReader(attrName))
            {
                return true;
            }
        }
        else
        {
            if (mReaderVec[i].second->GetAttributeReader(attrName))
            {
                return true;
            }
        }
    }
    return false;
}

IE_NAMESPACE_END(partition);

