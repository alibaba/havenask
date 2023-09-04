#include "indexlib/merger/test/segment_directory_creator.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SegmentDirectoryCreator);

SegmentDirectoryCreator::SegmentDirectoryCreator() {}

SegmentDirectoryCreator::~SegmentDirectoryCreator() {}

SegmentDirectoryPtr SegmentDirectoryCreator::CreateSegmentDirectory(const std::string& dataStr,
                                                                    const file_system::DirectoryPtr& rootDir)
{
    Version version = SegmentDirectoryCreator::MakeVersion(dataStr, rootDir);
    return SegmentDirectoryPtr(new SegmentDirectory(rootDir, version));
}

Version SegmentDirectoryCreator::MakeVersion(const std::string& dataStr, const file_system::DirectoryPtr& rootDir,
                                             bool hasSub)
{
    Version version;

    StringTokenizer st(dataStr, ":", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    versionid_t versionId;
    if (!StringUtil::strToInt32(st[0].c_str(), versionId)) {
        assert(false);
        return version;
    }
    version.SetVersionId(versionId);

    int64_t timestamp;
    if (!StringUtil::strToInt64(st[1].c_str(), timestamp)) {
        assert(false);
        return version;
    }
    version.SetTimestamp(timestamp);

    if (st.getNumTokens() < 3) {
        return version;
    }

    StringTokenizer st2(st[2], ",", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < st2.getNumTokens(); i++) {
        segmentid_t segId;
        if (!StringUtil::strToInt32(st2[i].c_str(), segId)) {
            assert(false);
            return version;
        }
        version.AddSegment(segId);

        auto segDir = rootDir->MakeDirectory(version.GetSegmentDirName(version[i]));
        assert(segDir != nullptr);
        if (segDir->IsExist(SEGMENT_INFO_FILE_NAME)) {
            segDir->RemoveFile(SEGMENT_INFO_FILE_NAME);
        }
        SegmentInfo info;
        /* we use segid as doc count by default */
        info.docCount = segId;
        info.Store(segDir);

        if (hasSub) {
            auto subSegDir = segDir->MakeDirectory("sub_segment");
            assert(subSegDir != nullptr);
            if (subSegDir->IsExist(SEGMENT_INFO_FILE_NAME)) {
                subSegDir->RemoveFile(SEGMENT_INFO_FILE_NAME);
            }
            info.Store(subSegDir);
        }
    }
    return version;
}
}} // namespace indexlib::merger
