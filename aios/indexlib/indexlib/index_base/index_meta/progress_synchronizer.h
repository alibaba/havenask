#ifndef __INDEXLIB_PROGRESS_SYNCHRONIZER_H
#define __INDEXLIB_PROGRESS_SYNCHRONIZER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/document/locator.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);

IE_NAMESPACE_BEGIN(index_base);

class ProgressSynchronizer
{
public:
    ProgressSynchronizer();
    ~ProgressSynchronizer();
    
public:
    // max ts & latest src with min offset locator
    void Init(const std::vector<Version>& versions);
    void Init(const std::vector<SegmentInfo>& segInfos);

    int64_t GetTimestamp() const { return mTimestamp; }
    const document::Locator GetLocator() const { return mLocator; }
    uint32_t GetFormatVersion() const  { return mFormatVersion; }

private:
    int64_t mTimestamp;
    document::Locator mLocator;
    uint32_t mFormatVersion;;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ProgressSynchronizer);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PROGRESS_SYNCHRONIZER_H
