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
#include "indexlib/partition/reader_container.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReaderContainer);

ReaderContainer::ReaderContainer() {}

ReaderContainer::~ReaderContainer() {}

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
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        if (mReaderVec[i].first == version) {
            return true;
        }
    }
    return false;
}

bool ReaderContainer::HasReader(versionid_t versionId) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        if (mReaderVec[i].first.GetVersionId() == versionId) {
            return true;
        }
    }
    return false;
}

void ReaderContainer::GetIncVersions(vector<Version>& versions) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        versions.push_back(mReaderVec[i].second->GetOnDiskVersion());
    }
}

bool ReaderContainer::IsUsingSegment(segmentid_t segmentId) const
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        if (mReaderVec[i].second->IsUsingSegment(segmentId)) {
            return true;
        }
    }
    return false;
}

IndexPartitionReaderPtr ReaderContainer::GetOldestReader() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty()) {
        return (*mReaderVec.begin()).second;
    }
    return IndexPartitionReaderPtr();
}

IndexPartitionReaderPtr ReaderContainer::GetLatestReader(versionid_t versionId) const
{
    ScopedLock lock(mReaderVecLock);
    int idx = (int)mReaderVec.size() - 1;
    for (; idx >= 0; idx--) {
        if (mReaderVec[idx].second->GetIncVersionId() == versionId) {
            return mReaderVec[idx].second;
        }
    }
    return IndexPartitionReaderPtr();
}

IndexPartitionReaderPtr ReaderContainer::PopOldestReader()
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty()) {
        IndexPartitionReaderPtr oldestReader = (*mReaderVec.begin()).second;
        mReaderVec.erase(mReaderVec.begin());
        return oldestReader;
    }
    return IndexPartitionReaderPtr();
}

versionid_t ReaderContainer::GetLatestReaderVersion() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty()) {
        return mReaderVec.back().second->GetVersion().GetVersionId();
    }
    return INVALID_VERSION;
}

versionid_t ReaderContainer::GetOldestReaderVersion() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty()) {
        return (*mReaderVec.begin()).second->GetVersion().GetVersionId();
    }
    return INVALID_VERSION;
}

void ReaderContainer::EvictOldestReader()
{
    mReaderVecLock.lock();
    if (!mReaderVec.empty()) {
        auto& [version, readerPtr] = *mReaderVec.begin();
        // Q: why not just reset readerPtr outside the lock?
        // A: in case EvictOldestReader() may be called in multi-thread in the future, since ptr reset() is not atomic.
        auto readerHolder = readerPtr;
        mReaderVec.erase(mReaderVec.begin());
        mReaderVecLock.unlock();
        readerHolder.reset();
        mReaderVecLock.lock();
        ResetSwitchRtSegRange();
    }
    mReaderVecLock.unlock();
}

bool ReaderContainer::EvictOldReaders()
{
    vector<IndexPartitionReaderPtr> readerToRelease;
    {
        ScopedLock lock(mReaderVecLock);
        for (auto& [version, readerPtr] : mReaderVec) {
            if (readerPtr.use_count() == 1) {
                readerToRelease.emplace_back(readerPtr);
                readerPtr.reset();
            } else {
                break;
            }
        }
        if (!readerToRelease.empty()) {
            ReaderVector tmp;
            for (auto& [version, readerPtr] : mReaderVec) {
                if (readerPtr) {
                    tmp.emplace_back(version, readerPtr);
                }
            }
            mReaderVec.swap(tmp);
            ResetSwitchRtSegRange();
        }
    }
    for (auto& readerPtr : readerToRelease) {
        assert(readerPtr.use_count() == 1);
        auto schemaName = readerPtr->GetSchema()->GetSchemaName();
        versionid_t versionId = readerPtr->GetVersion().GetVersionId();
        IE_LOG(INFO, "begin clean reader [%s][%d]", schemaName.c_str(), versionId);
        readerPtr.reset();
        IE_LOG(INFO, "finish clean reader [%s][%d]", schemaName.c_str(), versionId);
    }
    return !readerToRelease.empty();
}

size_t ReaderContainer::Size() const
{
    ScopedLock lock(mReaderVecLock);
    return mReaderVec.size();
}

int64_t ReaderContainer::GetLatestReaderIncVersionTimestamp() const
{
    ScopedLock lock(mReaderVecLock);
    if (!mReaderVec.empty()) {
        return mReaderVec.back().second->GetOnDiskVersion().GetTimestamp();
    }
    return INVALID_TIMESTAMP;
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
    for (size_t i = 0; i < mReaderVec.size(); i++) {
        auto onlineReader = mReaderVec[i].second;
        const vector<segmentid_t>& switchRtSegIds = onlineReader->GetSwitchRtSegments();
        if (switchRtSegIds.empty()) {
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
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        if (isSub) {
            if (mReaderVec[i].second->GetSubPartitionReader()->GetAttributeReader(attrName)) {
                return true;
            }
        } else {
            if (mReaderVec[i].second->GetAttributeReader(attrName)) {
                return true;
            }
        }
    }
    return false;
}

bool ReaderContainer::GetNeedKeepDeployFiles(std::set<std::string>* whiteList)
{
    ScopedLock lock(mReaderVecLock);
    for (size_t i = 0; i < mReaderVec.size(); ++i) {
        auto versionDpDesc = mReaderVec[i].second->GetVersionDeployDescription();
        if (!versionDpDesc) {
            IE_LOG(ERROR,
                   "partition reader get null version deploy description in GetNeedKeepDeployFiles, versionid[%d]",
                   static_cast<int>(mReaderVec[i].first.GetVersionId()));
            return false;
        }
        if (!versionDpDesc->SupportFeature(
                indexlibv2::framework::VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST)) {
            IE_LOG(ERROR,
                   "version deploy description doesn't support DEPLOY_META_MANIFEST feature in GetNeedKeepDeployFiles, "
                   "versionid[%d]",
                   static_cast<int>(mReaderVec[i].first.GetVersionId()));
            return false;
        }
        versionDpDesc->GetLocalDeployFileList(whiteList);
    }
    return true;
}

}} // namespace indexlib::partition
