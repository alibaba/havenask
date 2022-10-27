#include <autil/StringUtil.h>
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, MergeConfigImpl);

MergeConfigImpl::MergeConfigImpl()
    : checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
    , enablePackageFile(false)
    , enableInPlanMultiSegmentParallel(true)
    , enableArchiveFile(false)
{}

MergeConfigImpl::MergeConfigImpl(const MergeConfigImpl& other)
    : splitSegmentConfig(other.splitSegmentConfig)
    , packageFileTagConfigList(other.packageFileTagConfigList)
    , checkpointInterval(other.checkpointInterval)
    , enablePackageFile(other.enablePackageFile)
    , enableInPlanMultiSegmentParallel(other.enableInPlanMultiSegmentParallel)
    , enableArchiveFile(other.enableArchiveFile)
{
}

MergeConfigImpl::~MergeConfigImpl() {}

void MergeConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merged_segment_split_strategy", splitSegmentConfig, splitSegmentConfig);
    json.Jsonize("checkpoint_interval", checkpointInterval, checkpointInterval);
    json.Jsonize("enable_package_file", enablePackageFile, enablePackageFile);
    json.Jsonize("enable_in_plan_multi_segment_parallel", enableInPlanMultiSegmentParallel,
        enableInPlanMultiSegmentParallel);
    packageFileTagConfigList.Jsonize(json);
    json.Jsonize("enable_archive_file", enableArchiveFile, enableArchiveFile);
}

void MergeConfigImpl::Check() const
{
}

void MergeConfigImpl::operator = (const MergeConfigImpl& other)
{
    splitSegmentConfig = other.splitSegmentConfig;
    checkpointInterval = other.checkpointInterval;
    enablePackageFile = other.enablePackageFile;
    packageFileTagConfigList = other.packageFileTagConfigList;
    enableInPlanMultiSegmentParallel = other.enableInPlanMultiSegmentParallel;
    enableArchiveFile = other.enableArchiveFile;
}

IE_NAMESPACE_END(config);

