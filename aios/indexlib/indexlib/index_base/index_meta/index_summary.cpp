#include <autil/StringUtil.h>
#include "indexlib/misc/exception.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, IndexSummary);

void SegmentSummary::Jsonize(JsonWrapper& json) 
{
    json.Jsonize("id", id);
    json.Jsonize("size", size);
}

IndexSummary::IndexSummary(versionid_t _versionId, int64_t _timestamp)
    : mVersionId(_versionId)
    , mTimestamp(_timestamp)
    , mChanged(false)
{}

IndexSummary::IndexSummary()
    : mVersionId(INVALID_VERSION)
    , mTimestamp(INVALID_TIMESTAMP)
    , mChanged(false)
{}

IndexSummary::~IndexSummary()
{}

void IndexSummary::AddSegment(const SegmentSummary& info)
{
    mChanged = true;
    for (size_t i = 0; i < mSegmentSummarys.size(); i++)
    {
        if (mSegmentSummarys[i].id == info.id)
        {
            mSegmentSummarys[i] = info;
            return;
        }
    }
    mSegmentSummarys.push_back(info);
}

void IndexSummary::UpdateUsingSegment(const SegmentSummary& info)
{
    if (!mChanged)
    {
        mUsingSegments.clear();
    }
    mChanged = true;
    if (find(mUsingSegments.begin(), mUsingSegments.end(), info.id) == mUsingSegments.end())
    {
        mUsingSegments.push_back(info.id);
    }
    AddSegment(info);
}

void IndexSummary::RemoveSegment(segmentid_t segId)
{
    mChanged = true;
    mUsingSegments.erase(
            remove(mUsingSegments.begin(), mUsingSegments.end(), segId),
            mUsingSegments.end());

    auto it = remove_if(mSegmentSummarys.begin(), mSegmentSummarys.end(),
                        [&segId](SegmentSummary s) { return s.id == segId; });
    mSegmentSummarys.erase(it, mSegmentSummarys.end());
}

void IndexSummary::Commit(const string& rootDir)
{
    if (!mChanged)
    {
        return;
    }
    string dirPath = FileSystemWrapper::JoinPath(rootDir, INDEX_SUMMARY_INFO_DIR_NAME);
    string filePath = FileSystemWrapper::JoinPath(dirPath, GetFileName(mVersionId));
    IE_LOG(INFO, "store [%s]", filePath.c_str());
    FileSystemWrapper::AtomicStoreIgnoreExist(filePath, ToJsonString(*this));
}

void IndexSummary::Commit(const DirectoryPtr& rootDir)
{
    if (!mChanged)
    {
        return;
    }
    DirectoryPtr summaryInfoDir = rootDir->MakeDirectory(INDEX_SUMMARY_INFO_DIR_NAME);
    string fileName = GetFileName(mVersionId);
    summaryInfoDir->RemoveFile(fileName, true);
    IE_LOG(INFO, "store [%s] to [%s]", fileName.c_str(), summaryInfoDir->GetPath().c_str());
    summaryInfoDir->Store(fileName, ToJsonString(*this));
}

IndexSummary IndexSummary::Load(const DirectoryPtr& rootDir,
                                const vector<string>& versionFiles)
{
    if (versionFiles.empty())
    {
        return IndexSummary();
    }

    // versionFiles must be ordered by versionId
    versionid_t vid = VersionLoader::GetVersionId(*versionFiles.rbegin());
    versionid_t preVid = INVALID_VERSION;
    if (versionFiles.size() > 1)
    {
        preVid = VersionLoader::GetVersionId(versionFiles[versionFiles.size() - 2]);
    }
    return Load(rootDir, vid, preVid);
}

IndexSummary IndexSummary::Load(const string& rootDir,
                                const vector<string>& versionFiles)
{
    if (versionFiles.empty())
    {
        return IndexSummary();
    }

    // versionFiles must be ordered by versionId
    versionid_t vid = VersionLoader::GetVersionId(*versionFiles.rbegin());
    versionid_t preVid = INVALID_VERSION;
    if (versionFiles.size() > 1)
    {
        preVid = VersionLoader::GetVersionId(versionFiles[versionFiles.size() - 2]);
    }
    return Load(rootDir, vid, preVid);
}

IndexSummary IndexSummary::Load(const DirectoryPtr& rootDir,
                                versionid_t versionId, versionid_t preVersionId)
{
    IndexSummary summary;
    if (summary.Load(rootDir, FileSystemWrapper::JoinPath(
                            INDEX_SUMMARY_INFO_DIR_NAME,
                            IndexSummary::GetFileName(versionId))))
    {
        return summary;
    }

    Version version;
    VersionLoader::GetVersion(rootDir, version, versionId);
    if (preVersionId != INVALID_VERSION)
    {
        if (summary.Load(rootDir, FileSystemWrapper::JoinPath(
                                INDEX_SUMMARY_INFO_DIR_NAME,
                                IndexSummary::GetFileName(preVersionId))))
        {
            vector<SegmentSummary> segSummarys =
                GetSegmentSummaryForVersion(rootDir, version);
            for (auto& segSum : segSummarys)
            {
                summary.UpdateUsingSegment(segSum);
            }
            summary.SetVersionId(version.GetVersionId());
            summary.SetTimestamp(version.GetTimestamp());
            return summary;
        }
    }
    
    // list segment & read deploy index
    vector<SegmentSummary> versionSegments;
    vector<SegmentSummary> allSegments = GetSegmentSummaryFromRootDir(
            rootDir, version, versionSegments);
    for (auto &seg : allSegments)
    {
        summary.AddSegment(seg);
    }
    for (auto &seg : versionSegments)
    {
        summary.UpdateUsingSegment(seg);
    }
    summary.SetVersionId(version.GetVersionId());
    summary.SetTimestamp(version.GetTimestamp());
    return summary;
}

IndexSummary IndexSummary::Load(const string& rootDir,
                                versionid_t versionId, versionid_t preVersionId)
{
    string summaryDir = FileSystemWrapper::JoinPath(rootDir, INDEX_SUMMARY_INFO_DIR_NAME);
    IndexSummary summary;
    if (summary.Load(FileSystemWrapper::JoinPath(summaryDir, IndexSummary::GetFileName(versionId))))
    {
        return summary;
    }

    Version version;
    VersionLoader::GetVersion(rootDir, version, versionId);
    if (preVersionId != INVALID_VERSION)
    {
        if (summary.Load(FileSystemWrapper::JoinPath(summaryDir,
                                IndexSummary::GetFileName(preVersionId))))
        {
            vector<SegmentSummary> segSummarys =
                GetSegmentSummaryForVersion(rootDir, version);
            for (auto& segSum : segSummarys)
            {
                summary.UpdateUsingSegment(segSum);
            }
            summary.SetVersionId(version.GetVersionId());
            summary.SetTimestamp(version.GetTimestamp());
            return summary;
        }
    }
    // list segment & read deploy index
    vector<SegmentSummary> versionSegments;
    vector<SegmentSummary> allSegments = GetSegmentSummaryFromRootDir(
            rootDir, version, versionSegments);
    for (auto &seg : allSegments)
    {
        summary.AddSegment(seg);
    }
    for (auto &seg : versionSegments)
    {
        summary.UpdateUsingSegment(seg);
    }
    summary.SetVersionId(version.GetVersionId());
    summary.SetTimestamp(version.GetTimestamp());
    return summary;
}

bool IndexSummary::Load(const string& filePath)
{
    string content;
    if (!FileSystemWrapper::AtomicLoad(filePath, content, true))
    {
        IE_LOG(INFO, "file [%s] load fail: not exist!", filePath.c_str());
        return false;
    }

    FromJsonString(*this, content);
    return true;
}

bool IndexSummary::Load(const DirectoryPtr& dir, const string& fileName)
{
    string content;
    try
    {
        dir->Load(fileName, content, true);
    }
    catch (const NonExistException& e)
    {
        IE_LOG(INFO, "file [%s] load fail in dir [%s]: not exist!",
               fileName.c_str(), dir->GetPath().c_str());
        return false;
    }
    
    FromJsonString(*this, content);
    return true;
}

string IndexSummary::GetFileName(versionid_t id)
{
    return string(INDEX_SUMMARY_FILE_PREFIX) + "." + StringUtil::toString(id);
}

void IndexSummary::Jsonize(JsonWrapper& json) 
{
    json.Jsonize("version_id", mVersionId);
    json.Jsonize("timestamp", mTimestamp);
    json.Jsonize("current_version_segments", mUsingSegments);
    json.Jsonize("ondisk_segment_summary", mSegmentSummarys);
}

vector<SegmentSummary> IndexSummary::GetSegmentSummaryForVersion(
        const DirectoryPtr& rootDir, const Version& version)
{
    vector<SegmentSummary> ret;
    DeployIndexMeta indexMeta;
    bool loadMetaFlag = indexMeta.Load(rootDir,
            DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId()));
    auto iter = version.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segId = iter.Next();
        string segFileName = version.GetSegmentDirName(segId);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;
        if (loadMetaFlag)
        {
            for (auto& fileInfo : indexMeta.deployFileMetas)
            {
                if (fileInfo.isFile() && fileInfo.isValid() &&
                    fileInfo.filePath.find(segFileName) != string::npos)
                {
                    segSummary.size += fileInfo.fileLength;;
                }
            }
        }
        else
        {
            DirectoryPtr segDir = rootDir->GetDirectory(segFileName, true);
            if (!DeployIndexWrapper::GetSegmentSize(segDir, true, segSummary.size))
            {
                IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!", segId,
                       segDir->GetPath().c_str());
                segSummary.size = -1;
            }
        }
        ret.push_back(segSummary);
    }
    return ret;
}

vector<SegmentSummary> IndexSummary::GetSegmentSummaryForVersion(
        const string& rootDir, const Version& version)
{
    vector<SegmentSummary> ret;
    DeployIndexMeta indexMeta;
    bool loadMetaFlag = indexMeta.Load(
            FileSystemWrapper::JoinPath(rootDir,
                    DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId())));
    auto iter = version.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segId = iter.Next();
        string segFileName = version.GetSegmentDirName(segId);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;
        if (loadMetaFlag)
        {
            for (auto& fileInfo : indexMeta.deployFileMetas)
            {
                if (fileInfo.isFile() && fileInfo.isValid() &&
                    fileInfo.filePath.find(segFileName) != string::npos)
                {
                    segSummary.size += fileInfo.fileLength;;
                }
            }
        }
        else
        {
            string segPath = FileSystemWrapper::JoinPath(rootDir, segFileName);
            if (!DeployIndexWrapper::GetSegmentSize(segPath, true, segSummary.size))
            {
                IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!",
                       segId, segPath.c_str());
                segSummary.size = -1;
            }
        }
        ret.push_back(segSummary);
    }
    return ret;
}

vector<SegmentSummary> IndexSummary::GetSegmentSummaryFromRootDir(
        const DirectoryPtr& rootDir, const Version& version,
        vector<SegmentSummary>& inVersionSegments)
{
    vector<SegmentSummary> ret;
    FileList fileList;
    VersionLoader::ListSegments(rootDir, fileList);
    for (auto &file : fileList)
    {
        segmentid_t segId = Version::GetSegmentIdByDirName(file);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;

        DirectoryPtr segDir = rootDir->GetDirectory(file, true);
        if (!DeployIndexWrapper::GetSegmentSize(segDir, true, segSummary.size))
        {
            IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!", segId,
                   segDir->GetPath().c_str());
            segSummary.size = -1;
        }

        if (version.HasSegment(segId)) {
            inVersionSegments.push_back(segSummary);
            ret.push_back(segSummary);
            continue;
        }
        
        if (segId < version.GetLastSegment()) {
            ret.push_back(segSummary);
        }
    }
    return ret;
}

vector<SegmentSummary> IndexSummary::GetSegmentSummaryFromRootDir(
        const string& rootDir, const Version& version,
        vector<SegmentSummary>& inVersionSegments)
{
    vector<SegmentSummary> ret;
    FileList fileList;
    VersionLoader::ListSegments(rootDir, fileList);
    for (auto &file : fileList)
    {
        segmentid_t segId = Version::GetSegmentIdByDirName(file);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;

        string segPath = FileSystemWrapper::JoinPath(rootDir, file);
        if (!DeployIndexWrapper::GetSegmentSize(segPath, true, segSummary.size))
        {
            IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!",
                   segId, segPath.c_str());
            segSummary.size = -1;
        }

        if (version.HasSegment(segId)) {
            inVersionSegments.push_back(segSummary);
            ret.push_back(segSummary);
            continue;
        }
        if (segId < version.GetLastSegment()) {
            ret.push_back(segSummary);
        }
    }
    return ret;
}

int64_t IndexSummary::GetTotalSegmentSize() const
{
    int64_t size = 0;
    for (size_t i = 0; i < mSegmentSummarys.size(); i++)
    {
        if (mSegmentSummarys[i].size > 0)
        {
            size += mSegmentSummarys[i].size;
        }
    }
    return size;
}

void IndexSummary::SetVersionId(versionid_t id)
{
    mVersionId = id;
    mChanged = true;
}

void IndexSummary::SetTimestamp(int64_t ts)
{
    mTimestamp = ts;
    mChanged = true;
}

IE_NAMESPACE_END(index_base);

