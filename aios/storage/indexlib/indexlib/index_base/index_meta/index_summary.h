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
#ifndef __INDEXLIB_INDEX_SUMMARY_H
#define __INDEXLIB_INDEX_SUMMARY_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

class SegmentSummary : public autil::legacy::Jsonizable
{
public:
    void Jsonize(JsonWrapper& json) override;

public:
    int64_t size = 0;
    segmentid_t id = INVALID_SEGMENTID;
};

class IndexSummary : public autil::legacy::Jsonizable
{
public:
    IndexSummary(versionid_t _versionId, int64_t _timestamp);
    IndexSummary();
    ~IndexSummary();

public:
    void AddSegment(const SegmentSummary& info);
    void UpdateUsingSegment(const SegmentSummary& info);
    void RemoveSegment(segmentid_t segId);

    void Commit(const std::string& rootDir, file_system::FenceContext* fenceContext);
    void Commit(const file_system::DirectoryPtr& rootDir);
    void Jsonize(JsonWrapper& json) override;

    bool Load(const std::string& filePath);
    bool Load(const file_system::DirectoryPtr& dir, const std::string& fileName);

    int64_t GetTotalSegmentSize() const;

    void SetVersionId(versionid_t id);
    void SetTimestamp(int64_t ts);

    const std::vector<segmentid_t>& GetUsingSegments() const { return mUsingSegments; }
    const std::vector<SegmentSummary>& GetSegmentSummarys() const { return mSegmentSummarys; }

public:
    // versionFiles mustbe ordered by versionId
    static IndexSummary Load(const file_system::DirectoryPtr& rootDir, const std::vector<std::string>& versionFiles);

    static IndexSummary Load(const file_system::DirectoryPtr& rootDir, versionid_t versionId,
                             versionid_t preVersionId = INVALID_VERSION);

    static IndexSummary Load(const file_system::DirectoryPtr& rootDir, const Version& version,
                             versionid_t preVersionId = INVALID_VERSION);

    static std::string GetFileName(versionid_t id);

private:
    static std::vector<SegmentSummary> GetSegmentSummaryForVersion(const file_system::DirectoryPtr& rootDir,
                                                                   const Version& version);

    static std::vector<SegmentSummary> GetSegmentSummaryFromRootDir(const file_system::DirectoryPtr& rootDir,
                                                                    const Version& version,
                                                                    std::vector<SegmentSummary>& inVersionSegments);

private:
    versionid_t mVersionId;
    int64_t mTimestamp;
    std::vector<segmentid_t> mUsingSegments;
    std::vector<SegmentSummary> mSegmentSummarys;
    bool mChanged;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSummary);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_INDEX_SUMMARY_H
