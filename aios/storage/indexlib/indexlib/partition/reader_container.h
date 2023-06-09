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
#ifndef __INDEXLIB_READER_CONTAINER_H
#define __INDEXLIB_READER_CONTAINER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"

namespace indexlib { namespace partition {

// ReaderContainer is not thread-safe, the user should guarrantee it!
class ReaderContainer
{
private:
    typedef std::pair<index_base::Version, IndexPartitionReaderPtr> ReaderPair;
    typedef std::vector<ReaderPair> ReaderVector;

public:
    ReaderContainer();
    virtual ~ReaderContainer();

public:
    void AddReader(const IndexPartitionReaderPtr& reader);
    // TODO: remove
    virtual bool HasReader(const index_base::Version& version) const;
    virtual bool HasReader(versionid_t versionId) const;
    // virtual for test
    virtual void GetIncVersions(std::vector<index_base::Version>& versions) const;
    bool IsUsingSegment(segmentid_t segmentId) const;
    IndexPartitionReaderPtr GetOldestReader() const;
    IndexPartitionReaderPtr GetLatestReader(versionid_t versionId) const;
    IndexPartitionReaderPtr PopOldestReader();
    versionid_t GetOldestReaderVersion() const;
    // virtual for test
    virtual versionid_t GetLatestReaderVersion() const;
    int64_t GetLatestReaderIncVersionTimestamp() const;
    void EvictOldestReader();
    size_t Size() const;
    void Close();
    void GetSwitchRtSegments(std::vector<segmentid_t>& segIds) const;
    bool HasAttributeReader(const std::string& attrName, bool isSub) const;
    bool EvictOldReaders();
    // virtual for test
    virtual bool GetNeedKeepDeployFiles(std::set<std::string>* whiteList);

private:
    void ResetSwitchRtSegRange();

private:
    ReaderVector mReaderVec;
    std::vector<segmentid_t> mSwitchRtSegments;
    mutable autil::ThreadMutex mReaderVecLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReaderContainer);
}} // namespace indexlib::partition

#endif //__INDEXLIB_READER_CONTAINER_H
