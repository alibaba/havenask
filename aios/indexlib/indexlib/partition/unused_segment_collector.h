#ifndef __INDEXLIB_UNUSED_SEGMENT_COLLECTOR_H
#define __INDEXLIB_UNUSED_SEGMENT_COLLECTOR_H

#include <tr1/memory>
#include <fslib/common/common_type.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/reader_container.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(partition);

class UnusedSegmentCollector
{
public:
    UnusedSegmentCollector();
    ~UnusedSegmentCollector();
public:
    static void Collect(
            const ReaderContainerPtr& readerContainer,
            const file_system::DirectoryPtr& directory,
            fslib::FileList& segments, bool localOnly);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UnusedSegmentCollector);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_UNUSED_SEGMENT_COLLECTOR_H
