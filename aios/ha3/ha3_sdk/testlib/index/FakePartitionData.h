#pragma once

#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/on_disk_partition_data.h"

namespace indexlib {
namespace partition {

class FakePartitionData : public OnDiskPartitionData {
public:
    FakePartitionData(versionid_t incVersion = 0)
        : OnDiskPartitionData(indexlib::plugin::PluginManagerPtr()) {
        _indexVersion.SetVersionId(0);
    }
    ~FakePartitionData() {}

private:
    FakePartitionData(const FakePartitionData &);
    FakePartitionData &operator=(const FakePartitionData &);

public:
    /* override */ indexlib::index_base::Version GetOnDiskVersion() const {
        return _indexVersion;
    }

private:
    indexlib::index_base::Version _indexVersion;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakePartitionData> FakePartitionDataPtr;

} // namespace partition
} // namespace indexlib
