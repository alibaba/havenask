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
#include "indexlib/index_base/meta_cache_preloader.h"

#include <unistd.h>

#include "autil/TimeUtility.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace fslib::cache;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MetaCachePreloader);

MetaCachePreloader::MetaCachePreloader() {}

MetaCachePreloader::~MetaCachePreloader() {}

void MetaCachePreloader::Load(const DirectoryPtr& indexDir, versionid_t version)
{
    std::string indexPath = indexDir->GetOutputPath();
    if (version == INVALID_VERSION) {
        return;
    }

    FSCacheModule* fsCache = FileSystem::getCacheModule();
    if (!fsCache) {
        return;
    }

    if (!fsCache->needCacheMeta(indexPath)) {
        return;
    }

    char* envParam = getenv("DISABLE_INDEX_META_PRELOADER");
    if (envParam && string(envParam) == "true") {
        IE_LOG(INFO, "disable index_meta preloader");
        return;
    }

    string indexFileListPath = DeployIndexWrapper::GetDeployMetaFileName(version);
    DeployIndexMeta indexMeta;
    bool isExist = false;
    if (!indexMeta.Load(indexDir->GetIDirectory(), indexFileListPath, &isExist) || !isExist) {
        return;
    }

    IE_LOG(INFO, "Begin preload metaCache from version [%d] in path [%s]", version, indexPath.c_str());
    int32_t count = 0;
    auto addPathMetaInfo = [&count, &fsCache, &indexPath](std::vector<FileInfo>& fileInfoVec) {
        for (auto& deployFileMeta : fileInfoVec) {
            if (!deployFileMeta.isValid()) {
                continue;
            }
            if (deployFileMeta.modifyTime == (uint64_t)-1) {
                deployFileMeta.modifyTime = (uint64_t)TimeUtility::currentTimeInSeconds();
            }
            string path = PathUtil::JoinPath(indexPath, deployFileMeta.filePath);
            if (fsCache->addPathMeta(path, deployFileMeta.fileLength, deployFileMeta.modifyTime,
                                     deployFileMeta.modifyTime, deployFileMeta.isDirectory())) {
                ++count;
            }
        }
    };

    addPathMetaInfo(indexMeta.deployFileMetas);
    addPathMetaInfo(indexMeta.finalDeployFileMetas);
    IE_LOG(INFO, "End preload metaCache from version [%d] in path [%s], total file count [%d]", version,
           indexPath.c_str(), count);
}
}} // namespace indexlib::index_base
