#ifndef __INDEXLIB_TIME_RANGE_INFO_H
#define __INDEXLIB_TIME_RANGE_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/legacy/jsonizable.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(index);

class TimeRangeInfo : public autil::legacy::Jsonizable
{
public:
    TimeRangeInfo();
    ~TimeRangeInfo();
public:
    void Update(uint64_t value);
    uint64_t GetMinTime() const { return mMinTime; }
    uint64_t GetMaxTime() const { return mMaxTime; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool Load(const file_system::DirectoryPtr& directory);
    void Store(const file_system::DirectoryPtr& directory);
    bool IsValid() const { return mMinTime <= mMaxTime; }
private:
    volatile uint64_t mMinTime;
    volatile uint64_t mMaxTime;
private:
    friend class TimeRangeInfoTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeRangeInfo);

inline void TimeRangeInfo::Update(uint64_t value)
{
    if (value < mMinTime)
    {
        mMinTime = value;
    }
    if (value > mMaxTime)
    {
        mMaxTime = value;
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIME_RANGE_INFO_H
