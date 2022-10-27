#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/util/path_util.h"
#include <unistd.h>
#include <autil/TimeUtility.h>
#include <fslib/fs/FileSystem.h>
#include <fslib/cache/FSCacheModule.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace fslib::cache;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MetaCachePreloader);

MetaCachePreloader::MetaCachePreloader() 
{
}

MetaCachePreloader::~MetaCachePreloader() 
{
}

void MetaCachePreloader::Load(const string& indexPath, versionid_t version)
{
    if (version == INVALID_VERSION)
    {
        return;
    }
    
    FSCacheModule* fsCache = FileSystem::getCacheModule();
    if (!fsCache)
    {
        return;
    }

    if (!fsCache->needCacheMeta(indexPath))
    {
        return;
    }

    char* envParam = getenv("DISABLE_INDEX_META_PRELOADER");
    if (envParam && string(envParam) == "true")
    {
        IE_LOG(INFO, "disable index_meta preloader");
        return;
    }

    string indexFileListPath = PathUtil::JoinPath(
            indexPath, DeployIndexWrapper::GetDeployMetaFileName(version));
    DeployIndexMeta indexMeta;
    if (!indexMeta.Load(indexFileListPath)) {
        return;
    }

    IE_LOG(INFO, "Begin preload metaCache from version [%d] in path [%s]",
           version, indexPath.c_str());
    int32_t count = 0;
    auto addPathMetaInfo = [&count, &fsCache, &indexPath]
                           (std::vector<FileInfo>& fileInfoVec)
    {
        for (auto& deployFileMeta : fileInfoVec)
        {
            if (!deployFileMeta.isValid())
            {
                continue;
            }
            if (deployFileMeta.modifyTime == (uint64_t)-1)
            {
                deployFileMeta.modifyTime = (uint64_t)TimeUtility::currentTimeInSeconds();
            }
            string path = PathUtil::JoinPath(indexPath, deployFileMeta.filePath);
            if (fsCache->addPathMeta(path, deployFileMeta.fileLength, deployFileMeta.modifyTime,
                            deployFileMeta.modifyTime, deployFileMeta.isDirectory()))
            {
                ++count;
            }
        }
    };

    addPathMetaInfo(indexMeta.deployFileMetas);
    addPathMetaInfo(indexMeta.finalDeployFileMetas);
    IE_LOG(INFO, "End preload metaCache from version [%d] in path [%s], total file count [%d]",
           version, indexPath.c_str(), count);
}

IE_NAMESPACE_END(index_base);

