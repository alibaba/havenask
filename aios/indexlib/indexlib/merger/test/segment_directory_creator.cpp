#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "autil/StringUtil.h"
#include "autil/StringTokenizer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SegmentDirectoryCreator);

SegmentDirectoryCreator::SegmentDirectoryCreator() 
{
}

SegmentDirectoryCreator::~SegmentDirectoryCreator() 
{
}

SegmentDirectoryPtr SegmentDirectoryCreator::CreateSegmentDirectory(
        const std::string& dataStr, const std::string& rootDir)
{
    Version version = SegmentDirectoryCreator::MakeVersion(dataStr, rootDir);
    return SegmentDirectoryPtr(new SegmentDirectory(rootDir, version));
}

Version SegmentDirectoryCreator::MakeVersion(const std::string& dataStr, 
        const std::string& rootDir, bool hasSub)
{
    Version version;

    StringTokenizer st(dataStr, ":", StringTokenizer::TOKEN_IGNORE_EMPTY | 
                       StringTokenizer::TOKEN_TRIM);
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

    if (st.getNumTokens() < 3)
    {
        return version;
    }

    StringTokenizer st2(st[2], ",", StringTokenizer::TOKEN_TRIM |
                        StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < st2.getNumTokens(); i++)
    {
        segmentid_t segId;
        if (!StringUtil::strToInt32(st2[i].c_str(), segId)) 
        {
            assert(false);
            return version;
        }
        version.AddSegment(segId);

        string segPath = FileSystemWrapper::JoinPath(rootDir, version.GetSegmentDirName(version[i]));
        FileSystemWrapper::MkDirIfNotExist(segPath);
        string segInfoPath = FileSystemWrapper::JoinPath(segPath, SEGMENT_INFO_FILE_NAME);
        if (FileSystemWrapper::IsExist(segInfoPath))
        {
            FileSystemWrapper::DeleteDir(segInfoPath);
        }
        SegmentInfo info;
        /* we use segid as doc count by default */
        info.docCount = segId;
        info.Store(segInfoPath);        

        if (hasSub)
        {
            string subSegPath = FileSystemWrapper::JoinPath(
                    segPath, "sub_segment");
            string subSegInfoPath = FileSystemWrapper::JoinPath(
                    subSegPath, SEGMENT_INFO_FILE_NAME);
            if (FileSystemWrapper::IsExist(subSegInfoPath))
            {
                FileSystemWrapper::DeleteDir(subSegInfoPath);
            }
            info.Store(subSegInfoPath);
        }
    }
    return version;
}

IE_NAMESPACE_END(merger);

