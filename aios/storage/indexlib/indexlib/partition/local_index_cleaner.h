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
#pragma once

#include <memory>

#include "autil/Lock.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace partition {

// IndexCleaner is used to clean up index files whose versions are outside of KeepVersionCount.
class LocalIndexCleaner
{
public:
    /* @param localIndexRoot: the index root need to be clean, for now is LOCAL://xxx
     * @param loadVersionIndexRoot: the index root where can read version files
     */
    LocalIndexCleaner(const std::string& localIndexRoot, const std::string& loadVersionIndexRoot,
                      uint32_t keepVersionCount = INVALID_KEEP_VERSION_COUNT,
                      const ReaderContainerPtr& readerContainer = ReaderContainerPtr());
    ~LocalIndexCleaner() = default;

public:
    bool Clean(const std::vector<versionid_t>& keepVersionIds);
    bool CleanUnreferencedIndexFiles(const index_base::Version& targetVersion,
                                     const std::set<std::string>& toKeepFiles);

private:
    void DoClean(const std::vector<versionid_t>& keepVersionIds, bool cleanAfterMaxKeepVersionFiles);
    void CleanFiles(const fslib::FileList& fileList, const std::set<index_base::Version>& keepVersions,
                    bool cleanAfterMaxKeepVersionFiles);
    void CleanPatchIndexSegmentFiles(const std::string& patchDirName, const std::set<segmentid_t>& keepSegmentIds,
                                     segmentid_t maxCanCleanSegmentId, schemaid_t patchSchemaId);
    const index_base::Version& GetVersion(versionid_t versionId);

    bool CleanUnreferencedSegments(const index_base::Version& targetVersion, const std::set<std::string>& toKeepFiles);
    bool CleanUnreferencedLocalFiles(const index_base::Version& targetVersion, const std::set<std::string>& whiteList);
    bool ListFilesInSegmentDirs(const index_base::Version& targetVersion, std::set<std::string>* files);

private:
    static bool PrefixMatchAny(const std::string& prefix, const std::set<std::string>& candidates);

private:
    mutable autil::ThreadMutex mLock;

    std::string mLocalIndexRoot;
    std::string mLoadVersionIndexRoot;
    uint32_t mKeepVersionCount;
    ReaderContainerPtr mReaderContainer;
    std::set<index_base::Version> mVersionCache;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalIndexCleaner);
}} // namespace indexlib::partition
