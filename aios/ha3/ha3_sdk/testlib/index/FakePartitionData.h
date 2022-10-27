#ifndef ISEARCH_FAKEPARTITIONDATA_H
#define ISEARCH_FAKEPARTITIONDATA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/partition/on_disk_partition_data.h>

IE_NAMESPACE_BEGIN(partition);

class FakePartitionData : public OnDiskPartitionData
{
public:
    FakePartitionData(versionid_t incVersion = 0)
    : OnDiskPartitionData(IE_NAMESPACE(plugin)::PluginManagerPtr())
    {
        _indexVersion.SetVersionId(0);
    }
    ~FakePartitionData() {}
private:
    FakePartitionData(const FakePartitionData &);
    FakePartitionData& operator=(const FakePartitionData &);
public:
/* override */ IE_NAMESPACE(index_base)::Version GetOnDiskVersion() const {return _indexVersion;}

private:
    IE_NAMESPACE(index_base)::Version _indexVersion;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakePartitionData);
IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEPARTITIONDATA_H
