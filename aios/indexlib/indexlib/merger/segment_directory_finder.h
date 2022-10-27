#ifndef __INDEXLIB_SEGMENT_DIRECTORY_FINDER_H
#define __INDEXLIB_SEGMENT_DIRECTORY_FINDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(merger);

class SegmentDirectoryFinder
{
public:
    SegmentDirectoryFinder();
    ~SegmentDirectoryFinder();
public:
    static std::string GetAttributeDirPath(const SegmentDirectoryPtr& segDir,
            const config::AttributeConfig* attrConfig, segmentid_t segId);

    static segmentid_t GetSegmentIdFromDelMapDataFile(
            const SegmentDirectoryPtr& segDir,
            segmentid_t segmentId,
            const std::string& dataFileName);

    static void ListAttrPatch(const SegmentDirectory* segDir,
                              const std::string& attrName, segmentid_t segId, 
                              std::vector<std::pair<std::string, segmentid_t> >& patches);

    static void ListAttrPatch(const SegmentDirectory* segDir,
                              const std::string& attrName, segmentid_t segId, 
                               std::vector<std::string>& patches);

    static std::string MakeSegmentPath(const std::string& rootDir, 
                                       segmentid_t segId,
                                       const index_base::Version& version,
                                       bool isSub = false);

private:
    static void InnerListPatchFiles(
            const SegmentDirectory* segDir,
            const std::string& dirName, const std::string& patchSuffixName,
            const std::string& patchFieldName, segmentid_t segId, 
            std::vector<std::pair<std::string, segmentid_t> >& patches);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryFinder);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SEGMENT_DIRECTORY_FINDER_H
