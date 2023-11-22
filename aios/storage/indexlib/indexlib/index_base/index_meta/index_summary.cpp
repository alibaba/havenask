/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index_base/index_meta/index_summary.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
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
{
}

IndexSummary::IndexSummary() : mVersionId(INVALID_VERSIONID), mTimestamp(INVALID_TIMESTAMP), mChanged(false) {}

IndexSummary::~IndexSummary() {}

void IndexSummary::AddSegment(const SegmentSummary& info)
{
    mChanged = true;
    for (size_t i = 0; i < mSegmentSummarys.size(); i++) {
        if (mSegmentSummarys[i].id == info.id) {
            mSegmentSummarys[i] = info;
            return;
        }
    }
    mSegmentSummarys.push_back(info);
}

void IndexSummary::UpdateUsingSegment(const SegmentSummary& info)
{
    if (!mChanged) {
        mUsingSegments.clear();
    }
    mChanged = true;
    if (find(mUsingSegments.begin(), mUsingSegments.end(), info.id) == mUsingSegments.end()) {
        mUsingSegments.push_back(info.id);
    }
    AddSegment(info);
}

void IndexSummary::RemoveSegment(segmentid_t segId)
{
    mChanged = true;
    mUsingSegments.erase(remove(mUsingSegments.begin(), mUsingSegments.end(), segId), mUsingSegments.end());

    auto it = remove_if(mSegmentSummarys.begin(), mSegmentSummarys.end(),
                        [&segId](SegmentSummary s) { return s.id == segId; });
    mSegmentSummarys.erase(it, mSegmentSummarys.end());
}

void IndexSummary::Commit(const string& rootDir, FenceContext* fenceContext)
{
    if (!mChanged) {
        return;
    }
    string filePath = PathUtil::JoinPath(rootDir, INDEX_SUMMARY_INFO_DIR_NAME, GetFileName(mVersionId));
    IE_LOG(INFO, "store [%s]", filePath.c_str());
    auto ec = FslibWrapper::AtomicStore(filePath, ToJsonString(*this), true, fenceContext).Code();
    THROW_IF_FS_ERROR(ec, "atomic store [%s] failed", filePath.c_str());
}

void IndexSummary::Commit(const DirectoryPtr& rootDir)
{
    if (!mChanged) {
        return;
    }
    return Commit(rootDir->GetOutputPath(), rootDir->GetFenceContext().get());
}

IndexSummary IndexSummary::Load(const DirectoryPtr& rootDir, const vector<string>& versionFiles)
{
    if (versionFiles.empty()) {
        return IndexSummary();
    }

    // versionFiles must be ordered by versionId
    versionid_t vid = VersionLoader::GetVersionId(*versionFiles.rbegin());
    versionid_t preVid = INVALID_VERSIONID;
    if (versionFiles.size() > 1) {
        preVid = VersionLoader::GetVersionId(versionFiles[versionFiles.size() - 2]);
    }
    return Load(rootDir, vid, preVid);
}

IndexSummary IndexSummary::Load(const DirectoryPtr& rootDir, versionid_t versionId, versionid_t preVersionId)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, versionId);
    return Load(rootDir, version, preVersionId);
}

IndexSummary IndexSummary::Load(const DirectoryPtr& rootDir, const Version& version, versionid_t preVersionId)
{
    IndexSummary summary;
    if (summary.Load(rootDir, util::PathUtil::JoinPath(INDEX_SUMMARY_INFO_DIR_NAME,
                                                       IndexSummary::GetFileName(version.GetVersionId())))) {
        return summary;
    }

    if (preVersionId != INVALID_VERSIONID) {
        if (summary.Load(rootDir, util::PathUtil::JoinPath(INDEX_SUMMARY_INFO_DIR_NAME,
                                                           IndexSummary::GetFileName(preVersionId)))) {
            vector<SegmentSummary> segSummarys = GetSegmentSummaryForVersion(rootDir, version);
            for (auto& segSum : segSummarys) {
                summary.UpdateUsingSegment(segSum);
            }
            summary.SetVersionId(version.GetVersionId());
            summary.SetTimestamp(version.GetTimestamp());
            return summary;
        }
    }

    // list segment & read deploy index
    vector<SegmentSummary> versionSegments;
    vector<SegmentSummary> allSegments = GetSegmentSummaryFromRootDir(rootDir, version, versionSegments);
    for (auto& seg : allSegments) {
        summary.AddSegment(seg);
    }
    for (auto& seg : versionSegments) {
        summary.UpdateUsingSegment(seg);
    }
    summary.SetVersionId(version.GetVersionId());
    summary.SetTimestamp(version.GetTimestamp());
    return summary;
}

bool IndexSummary::Load(const DirectoryPtr& dir, const string& fileName)
{
    string fullPath = PathUtil::JoinPath(dir->GetOutputPath(), fileName);
    return Load(fullPath);
}

bool IndexSummary::Load(const string& filePath)
{
    string content;
    auto ec = FslibWrapper::AtomicLoad(filePath, content).Code();
    if (ec == FSEC_NOENT) {
        IE_LOG(INFO, "file [%s] load fail: not exist!", filePath.c_str());
        return false;
    }
    THROW_IF_FS_ERROR(ec, "load index summary from [%s] failed", filePath.c_str());
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

vector<SegmentSummary> IndexSummary::GetSegmentSummaryForVersion(const DirectoryPtr& rootDir, const Version& version)
{
    vector<SegmentSummary> ret;
    DeployIndexMeta indexMeta;
    bool loadMetaFlag =
        indexMeta.Load(rootDir->GetIDirectory(), DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId()));
    auto iter = version.CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segId = iter.Next();
        string segmentDirName = version.GetSegmentDirName(segId);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;
        if (loadMetaFlag) {
            for (auto& fileInfo : indexMeta.deployFileMetas) {
                if (fileInfo.isFile() && fileInfo.isValid() && fileInfo.filePath.find(segmentDirName) != string::npos) {
                    segSummary.size += fileInfo.fileLength;
                    ;
                }
            }
        } else {
            DirectoryPtr segDir = rootDir->GetDirectory(segmentDirName, false);
            if (segDir) {
                segSummary.size = DeployIndexWrapper::GetSegmentSize(segDir, true);
                if (segSummary.size < 0) {
                    IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!", segId,
                           segDir->DebugString().c_str());
                    segSummary.size = -1;
                }
            }
        }
        ret.push_back(segSummary);
    }
    return ret;
}

vector<SegmentSummary> IndexSummary::GetSegmentSummaryFromRootDir(const DirectoryPtr& rootDir, const Version& version,
                                                                  vector<SegmentSummary>& inVersionSegments)
{
    vector<SegmentSummary> ret;
    FileList fileList;
    VersionLoader::ListSegments(rootDir, fileList);
    for (auto& file : fileList) {
        segmentid_t segId = Version::GetSegmentIdByDirName(file);
        SegmentSummary segSummary;
        segSummary.id = segId;
        segSummary.size = 0;

        DirectoryPtr segDir = rootDir->GetDirectory(file, true);
        segSummary.size = DeployIndexWrapper::GetSegmentSize(segDir, true);
        if (segSummary.size < 0) {
            IE_LOG(WARN, "GetSegmentSize for segment [%d] in path [%s] fail!", segId, segDir->DebugString().c_str());
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
    for (size_t i = 0; i < mSegmentSummarys.size(); i++) {
        if (mSegmentSummarys[i].size > 0) {
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
}} // namespace indexlib::index_base
