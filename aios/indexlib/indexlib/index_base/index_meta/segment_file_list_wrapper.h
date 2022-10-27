#ifndef __INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H
#define __INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/index_file_list.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentInfo;
DEFINE_SHARED_PTR(SegmentInfo);

class SegmentFileListWrapper
{
private:
    SegmentFileListWrapper();
    ~SegmentFileListWrapper();

public:
    static bool Load(const std::string& path, IndexFileList& meta);
    static bool Load(const file_system::DirectoryPtr& directory, IndexFileList& meta);

    static bool Dump(const std::string& path, const std::string& lifecycle = "");
    static bool Dump(const std::string& path, const fslib::FileList& fileList,
                     const std::string& lifecycle = "");

    static void Dump(const file_system::DirectoryPtr& directory, const fslib::FileList& fileList,
        const SegmentInfoPtr& segmentInfo = SegmentInfoPtr(), const std::string& lifecycle = "");
    static void Dump(const file_system::DirectoryPtr& directory,
        const SegmentInfoPtr& segmentInfo = SegmentInfoPtr(), const std::string& lifecycle = "");

private:
    static bool needDumpLegacyFile();
    static bool IsExist(const std::string& path);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentFileListWrapper);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_FILE_LIST_WRAPPER_H
