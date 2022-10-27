#ifndef __INDEXLIB_ON_DISK_INDEX_CLEANER_H
#define __INDEXLIB_ON_DISK_INDEX_CLEANER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

IE_NAMESPACE_BEGIN(partition);

class OnDiskIndexCleaner : public common::Executor
{
public:
    OnDiskIndexCleaner(uint32_t keepVersionCount,
                       const ReaderContainerPtr& readerContainer,
                       const file_system::DirectoryPtr& directory);
    ~OnDiskIndexCleaner();

public:
    void Execute() override;

private:
    void ConstructNeedKeepVersionIdxAndSegments(
            const fslib::FileList& fileList, 
            size_t& firstKeepVersionIdx,
            std::set<segmentid_t>& needKeepSegments,
            std::set<schemavid_t>& needKeepSchemaId);

    segmentid_t CalculateMaxSegmentIdInAllVersions(
            const fslib::FileList& fileList);

    void CleanVersionFiles(size_t firstKeepVersionIdx,
                           const fslib::FileList& fileList);

    void CleanSegmentFiles(const file_system::DirectoryPtr& dir,
                           segmentid_t maxSegIdInAllVersions,
                           const std::set<segmentid_t>& needKeepSegments);

    void CleanPatchIndexSegmentFiles(segmentid_t maxSegInVersion,
            const std::set<segmentid_t>& needKeepSegment);

    void CleanUselessSchemaFiles(const std::set<schemavid_t>& needKeepSchemaId);

private:
    uint32_t mKeepVersionCount;
    ReaderContainerPtr mReaderContainer;
    file_system::DirectoryPtr mDirectory;
    bool mLocalOnly; // true for OnlinePartition

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnDiskIndexCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ON_DISK_INDEX_CLEANER_H
