#ifndef __INDEXLIB_DUMP_STRATEGY_H
#define __INDEXLIB_DUMP_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"

IE_NAMESPACE_BEGIN(merger);

class DumpStrategy
{
public:
    DumpStrategy(const SegmentDirectoryPtr &segDir)
        : mRootDir(segDir->GetRootDir())
        , mDumppingVersion(segDir->GetVersion())
        , mIsDirty(false)
    {
    }

    DumpStrategy(const std::string &rootDir,
                 const index_base::Version& version)
        : mRootDir(rootDir)
        , mDumppingVersion(version)
        , mIsDirty(false)
    {
    }
    ~DumpStrategy() {}

public:
    segmentid_t GetLastSegmentId() const { return mDumppingVersion.GetLastSegment(); }
    void RemoveSegment(segmentid_t segId)
    { mDumppingVersion.RemoveSegment(segId); mIsDirty = true; }
    void AddMergedSegment(segmentid_t segId)
    { mDumppingVersion.AddSegment(segId); mIsDirty = true; }
    void AddMergedSegment(segmentid_t segId, const index_base::SegmentTopologyInfo& topoInfo)
    { mDumppingVersion.AddSegment(segId, topoInfo); mIsDirty = true; }
    const index_base::Version& GetVersion() const { return mDumppingVersion; }
    void IncreaseLevelCursor(uint32_t levelIdx)
    {
        index_base::LevelInfo& levelInfo = mDumppingVersion.GetLevelInfo();
        mIsDirty = true;
        if (levelIdx > 0 && levelIdx < levelInfo.GetLevelCount())
        {
            index_base::LevelMeta& levelMeta = levelInfo.levelMetas[levelIdx];
            levelMeta.cursor = (levelMeta.cursor + 1) % levelMeta.segments.size();
        }
    }
    void IncVersionId() { mDumppingVersion.IncVersionId(); }
    const std::string &GetRootDir() const { return mRootDir; }
    void Reload(int64_t ts, const document::Locator& locator)
    {
        index_base::VersionLoader::GetVersion(mRootDir, mDumppingVersion, INVALID_VERSION);
        mDumppingVersion.SetTimestamp(ts);
        mDumppingVersion.SetLocator(locator);
    }

    bool IsDirty() const { return mIsDirty; }

    //just for test
    index_base::Version getDumpingVersion() { return mDumppingVersion; }
    void RevertVersion(const index_base::Version& version)
    { mDumppingVersion = version; }
private:
    std::string mRootDir;
    index_base::Version mDumppingVersion;
    bool mIsDirty;
};

DEFINE_SHARED_PTR(DumpStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DUMP_STRATEGY_H
