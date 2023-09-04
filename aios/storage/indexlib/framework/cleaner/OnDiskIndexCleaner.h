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
#include <mutex>
#include <set>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

class OnDiskIndexCleaner
{
public:
    OnDiskIndexCleaner(const std::string& localIndexRoot, const std::string& tabletName, uint32_t keepVersionCount,
                       TabletReaderContainer* tabletReaderContainer)
        : _localIndexRoot(localIndexRoot)
        , _tabletName(tabletName)
        , _keepVersionCount(keepVersionCount)
        , _tabletReaderContainer(tabletReaderContainer)
    {
        if (_keepVersionCount == INVALID_KEEP_VERSION_COUNT) {
            _keepVersionCount = 1;
        }
        assert(_tabletReaderContainer != nullptr);
    }
    virtual ~OnDiskIndexCleaner() = default;

    //如果要保留的版本在本地不全存在，不会执行清理操作，避免误删分发中的索引
    //内部会做保护，外面调用清理的时候需要保证清理的版本在本地都有
    Status Clean(const std::vector<versionid_t>& reservedVersions);

    Status Clean(const std::shared_ptr<indexlib::file_system::Directory>& rootDirInFs,
                 const indexlibv2::framework::Version& targetVersion, const std::set<std::string>& toKeepFiles);

private:
    Status CollectAllRelatedVersion(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                    const std::set<versionid_t>& reservedVersionIds, std::vector<Version>* versions,
                                    bool* isAllReservedVersionExist);
    std::vector<std::string> CaculateNeedCleanFileList(const std::vector<std::string>& allFileList,
                                                       const std::set<std::string>& keepFiles,
                                                       const std::set<segmentid_t>& keepSegments,
                                                       const std::set<versionid_t>& keepVersions);

    Status CollectReservedFilesAndDirs(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                       const std::set<versionid_t>& reservedVersionIdSet,
                                       std::set<std::string>* keepFiles, std::set<segmentid_t>* keepSegments,
                                       std::set<versionid_t>* keepVersions, bool* isDeploying);

    Status CleanFiles(const std::vector<std::string>& allFileList,
                      const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                      const std::vector<std::string>& needCleanFileList);

    Status CollectVersionFromLocal(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                   std::vector<Version>* allVersions);

    bool CleanUnreferencedSegments(const std::shared_ptr<indexlib::file_system::Directory>& rootDirInFs,
                                   const std::shared_ptr<indexlib::file_system::IDirectory>& physicalRootDir,
                                   const indexlibv2::framework::Version& targetVersion,
                                   const std::set<std::string>& toKeepFiles);
    bool CleanUnreferencedLocalFiles(const std::shared_ptr<indexlib::file_system::Directory>& rootDirInFs,
                                     const std::shared_ptr<indexlib::file_system::IDirectory>& physicalRootDir,
                                     const indexlibv2::framework::Version& targetVersion,
                                     const std::set<std::string>& toKeepFiles);
    bool ListFilesInSegmentDirs(const std::shared_ptr<indexlib::file_system::Directory>& rootDirInFs,
                                const std::shared_ptr<indexlib::file_system::IDirectory>& physicalRootDir,
                                const indexlibv2::framework::Version& targetVersion, std::set<std::string>* files);

    bool PrefixMatchAny(const std::string& prefix, const std::set<std::string>& candidates);

private:
    std::mutex _mutex;
    const std::string _localIndexRoot;
    const std::string _tabletName;
    uint32_t _keepVersionCount;
    TabletReaderContainer* _tabletReaderContainer;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
