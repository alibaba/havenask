#ifndef __INDEXLIB_INDEX_SUMMARY_H
#define __INDEXLIB_INDEX_SUMMARY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

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
    
    void Commit(const std::string& rootDir);
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
    static IndexSummary Load(const file_system::DirectoryPtr& rootDir,
                             const std::vector<std::string>& versionFiles);

    // versionFiles mustbe ordered by versionId
    static IndexSummary Load(const std::string& rootDir,
                             const std::vector<std::string>& versionFiles);


    static IndexSummary Load(const file_system::DirectoryPtr& rootDir,
                             versionid_t versionId, versionid_t preVersionId = INVALID_VERSION);

    static IndexSummary Load(const std::string& rootDir,
                             versionid_t versionId, versionid_t preVersionId = INVALID_VERSION);
    
    static std::string GetFileName(versionid_t id);

private:
    static std::vector<SegmentSummary> GetSegmentSummaryForVersion(
            const file_system::DirectoryPtr& rootDir, const Version& version);

    static std::vector<SegmentSummary> GetSegmentSummaryForVersion(
            const std::string& rootDir, const Version& version);


    static std::vector<SegmentSummary> GetSegmentSummaryFromRootDir(
            const file_system::DirectoryPtr& rootDir, const Version& version,
            std::vector<SegmentSummary>& inVersionSegments);

    static std::vector<SegmentSummary> GetSegmentSummaryFromRootDir(
            const std::string& rootDir, const Version& version,
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

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEX_SUMMARY_H
