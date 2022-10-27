#ifndef __INDEXLIB_PARALLEL_BUILD_INFO_H
#define __INDEXLIB_PARALLEL_BUILD_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(index_base);

class ParallelBuildInfo : public autil::legacy::Jsonizable
{
public:
    ParallelBuildInfo();
    ParallelBuildInfo(uint32_t _parallelNum, uint32_t _batch, uint32_t _instanceId);
    ~ParallelBuildInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void CheckValid(const std::string &dir);
    bool IsParallelBuild() const;
    void StoreIfNotExist(const std::string& dir);//, segmentid_t lastSegmentId);
    void Load(const std::string& idr);
    std::string GetParallelInstancePath(const std::string& rootPath) const;
    std::string GetParallelPath(const std::string& rootPath) const;
    static bool IsExist(const std::string &dir);
    // for test
    static std::string GetParallelPath(const std::string& rootPath,
                                       uint32_t parallelNum, uint32_t batchId);
    static bool IsParallelBuildPath(const std::string& path);

    void SetBaseVersion(versionid_t _baseVersion) { baseVersion = _baseVersion; }
    versionid_t GetBaseVersion() const { return baseVersion; }

    bool operator==(const ParallelBuildInfo& info) const;
    bool operator!=(const ParallelBuildInfo& info) const { return !(*this == info); }
        
public:
    uint32_t parallelNum;
    uint32_t batchId;
    uint32_t instanceId;
    
private:
    versionid_t baseVersion;
    
    static const std::string PARALLEL_BUILD_INFO_FILE;
    static const std::string PARALLEL_BUILD_PREFIX;
    static const std::string PARALLEL_BUILD_INSTANCE_PREFIX;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelBuildInfo);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARALLEL_BUILD_INFO_H
