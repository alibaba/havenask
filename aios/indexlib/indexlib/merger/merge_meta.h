#ifndef __INDEXLIB_MERGE_META_H
#define __INDEXLIB_MERGE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"

IE_NAMESPACE_BEGIN(merger);

class MergeMeta
{
public:
    MergeMeta();
    virtual ~MergeMeta();
public:
    virtual void Store(const std::string &rootPath) const = 0;
    virtual bool Load(const std::string &rootPath) = 0;
    virtual bool LoadBasicInfo(const std::string &rootPath) = 0;
    virtual size_t GetMergePlanCount() const = 0;
    virtual std::vector<segmentid_t> GetTargetSegmentIds(size_t planIdx) const = 0;
    virtual int64_t EstimateMemoryUse() const = 0;
    
public:    
    const index_base::Version& GetTargetVersion() const { return mTargetVersion; }
    void SetTargetVersion(const index_base::Version& targetVersion)
    { mTargetVersion = targetVersion; }    

protected:
    index_base::Version mTargetVersion;
};

DEFINE_SHARED_PTR(MergeMeta);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_META_H
