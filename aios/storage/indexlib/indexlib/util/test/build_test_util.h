#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace util {

class BuildTestUtil
{
public:
    BuildTestUtil() = delete;
    ~BuildTestUtil() = delete;

public:
    static config::IndexPartitionOptions GetIndexPartitionOptionsForBuildMode(int buildMode)
    {
        config::IndexPartitionOptions options;
        SetupIndexPartitionOptionsForBuildMode(buildMode, &options);
        return options;
    }

    static void SetupIndexPartitionOptionsForBuildMode(int buildMode, config::IndexPartitionOptions* options)
    {
        switch (buildMode) {
        case 0: { // BUILD_MODE_STREAM
            options->GetBuildConfig(/*isOnline=*/false)
                .SetBatchBuild(/*isConsistentMode=*/true, /*enableBatchBuild=*/true);
            options->GetBuildConfig(/*isOnline=*/false)
                .SetBatchBuild(/*isConsistentMode=*/false, /*enableBatchBuild=*/true);
            options->GetBuildConfig(/*isOnline=*/true)
                .SetBatchBuild(/*isConsistentMode=*/true, /*enableBatchBuild=*/false);
            options->GetBuildConfig(/*isOnline=*/true)
                .SetBatchBuild(/*isConsistentMode=*/false, /*enableBatchBuild=*/false);
            return;
        }
        case 1: { // BUILD_MODE_CONSISTENT_BATCH
            options->GetBuildConfig(/*isOnline=*/false)
                .SetBatchBuild(/*isConsistentMode=*/true, /*enableBatchBuild=*/true);
            options->GetBuildConfig(/*isOnline=*/true)
                .SetBatchBuild(/*isConsistentMode=*/true, /*enableBatchBuild=*/true);
            return;
        }
        case 2: { // BUILD_MODE_INCONSISTENT_BATCH
            options->GetBuildConfig(/*isOnline=*/false)
                .SetBatchBuild(/*isConsistentMode=*/false, /*enableBatchBuild=*/true);
            options->GetBuildConfig(/*isOnline=*/true)
                .SetBatchBuild(/*isConsistentMode=*/false, /*enableBatchBuild=*/true);
            return;
        }
        default:
            assert(false);
        }
    }

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::util
