#include "indexlib/partition/index_roll_back_util.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/misc/exception.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include <beeper/beeper.h>

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexRollBackUtil);

IndexRollBackUtil::IndexRollBackUtil() 
{
}

IndexRollBackUtil::~IndexRollBackUtil() 
{
}

versionid_t IndexRollBackUtil::GetLatestOnDiskVersion(const string& indexRoot)
{
    Version version;
    try {
        VersionLoader::GetVersion(indexRoot, version, INVALID_VERSION);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "get latest version from [%s] failed", indexRoot.c_str());
        return INVALID_VERSION;
    }
    return version.GetVersionId();
}

bool IndexRollBackUtil::CreateRollBackVersion(
    const string& indexRoot,
    versionid_t sourceVersionId,
    versionid_t targetVersionId)
{
    try {
        Version sourceVersion;
        fslib::FileList segmentLists;
        VersionLoader::GetVersion(indexRoot, sourceVersion, sourceVersionId);
        VersionLoader::ListSegments(indexRoot, segmentLists);
        segmentid_t lastSegmentId = 0;    
        if (segmentLists.empty())
        {
            IE_LOG(WARN, "no segments found in [%s], "
                   "set lastSegmentId=0 in targetVersion[%d]",
                   indexRoot.c_str(), targetVersionId);
        }
        else
        {
            size_t segmentCount = segmentLists.size();
            lastSegmentId = Version::GetSegmentIdByDirName(segmentLists[segmentCount - 1]);
            if (lastSegmentId == INVALID_SEGMENTID) {
                IE_LOG(ERROR, "invalid segment name[%s] found in [%s]",
                       segmentLists[segmentCount-1].c_str(), indexRoot.c_str());
                IE_LOG(ERROR, "create roll back from version[%d] to version[%d] failed",
                       sourceVersionId, targetVersionId);
                BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                        "create roll back from version[%d] to version[%d] fail!",
                        sourceVersionId, targetVersionId);
                return false; 
            }
        }

        if (sourceVersion.GetSchemaVersionId() != DEFAULT_SCHEMAID)
        {
            PartitionPatchMeta patchMeta;
            patchMeta.Load(indexRoot, sourceVersion.GetVersionId());
            patchMeta.Store(indexRoot, targetVersionId);
        }

        Version latestVersion;
        VersionLoader::GetVersion(indexRoot, latestVersion, INVALID_VERSION);
                
        Version targetVersion(sourceVersion);
        targetVersion.SetVersionId(targetVersionId);
        targetVersion.SetLastSegmentId(lastSegmentId);
        DeployIndexWrapper::DumpDeployMeta(indexRoot, targetVersion);
        // write target version with overwrite flag
        targetVersion.Store(indexRoot, true);
        IndexSummary ret = IndexSummary::Load(indexRoot,
                targetVersion.GetVersionId(), latestVersion.GetVersionId());
        ret.Commit(indexRoot);
    }
    catch (const ExceptionBase &e)
    {
        IE_LOG(ERROR, "create roll back from version[%d] to version[%d] failed",
               sourceVersionId, targetVersionId);
        
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "create roll back from version[%d] to version[%d] fail!",
                sourceVersionId, targetVersionId);
        return false;
    }

    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "create roll back from version[%d] to version[%d] success!",
            sourceVersionId, targetVersionId);
    return true;
}

bool IndexRollBackUtil::CheckVersion(
    const string& indexRoot, versionid_t versionId)
{
    Version version;
    try {
        VersionLoader::GetVersion(indexRoot, version, versionId);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "get version[%d] from [%s] failed",
               versionId, indexRoot.c_str());
        return false;
    }
    return true;
}

IE_NAMESPACE_END(partition);

