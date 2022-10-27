#ifndef __INDEXLIB_ON_DISK_PARTITION_DATA_H
#define __INDEXLIB_ON_DISK_PARTITION_DATA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_info_holder.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);

IE_NAMESPACE_BEGIN(partition);

class OnDiskPartitionData;
DEFINE_SHARED_PTR(OnDiskPartitionData);

// used by reader
class OnDiskPartitionData : public index_base::PartitionData
{
public:
    OnDiskPartitionData(const plugin::PluginManagerPtr& pluginManager);
    OnDiskPartitionData(const OnDiskPartitionData& other);
    ~OnDiskPartitionData();
public:
    virtual void Open(const index_base::SegmentDirectoryPtr& segDir,
                      bool needDeletionMap = false);

    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;
    index_base::PartitionMeta GetPartitionMeta() const override;
    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override;
    const file_system::DirectoryPtr& GetRootDirectory() const override;

    Iterator Begin() const override;
    Iterator End() const override;

    index_base::SegmentData GetSegmentData(segmentid_t segId) const override;
    OnDiskPartitionData* Clone() override;
    index_base::PartitionDataPtr GetSubPartitionData() const override
    { return mSubPartitionData; }
    index::DeletionMapReaderPtr GetDeletionMapReader() const override
    { return mDeletionMapReader; }
    index::PartitionInfoPtr GetPartitionInfo() const override;

    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override
    { return mSegmentDirectory; }

    uint32_t GetIndexShardingColumnNum(
            const config::IndexPartitionOptions& options) const override;

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(
            segmentid_t lastLinkRtSegId) override;
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    std::string GetLastLocator() const override;

    const plugin::PluginManagerPtr& GetPluginManager() const override
    { return mPluginManager; } 
    
protected:
    OnDiskPartitionDataPtr CreatePartitionData();
    void InitPartitionMeta(const file_system::DirectoryPtr& directory);
    index::DeletionMapReaderPtr CreateDeletionMapReader();
    virtual PartitionInfoHolderPtr CreatePartitionInfoHolder() const;
    PartitionInfoHolderPtr GetPartitionInfoHolder() const
    { return mPartitionInfoHolder; }

protected:
    index_base::SegmentDirectoryPtr mSegmentDirectory;
    index_base::PartitionMeta mPartitionMeta;
    OnDiskPartitionDataPtr mSubPartitionData;
    index::DeletionMapReaderPtr mDeletionMapReader;
    PartitionInfoHolderPtr mPartitionInfoHolder;
    plugin::PluginManagerPtr mPluginManager;

private:
    friend class OnDiskPartitionDataTest;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ON_DISK_PARTITION_DATA_H
