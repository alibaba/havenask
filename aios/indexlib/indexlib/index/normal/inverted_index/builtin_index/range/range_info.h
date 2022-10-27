#ifndef __INDEXLIB_RANGE_INFO_H
#define __INDEXLIB_RANGE_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/legacy/jsonizable.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class RangeInfo : public autil::legacy::Jsonizable
{
public:
    RangeInfo();
    ~RangeInfo();
public:
    void Update(uint64_t value);
    uint64_t GetMinNumber() const { return mMinNumber; }
    uint64_t GetMaxNumber() const { return mMaxNumber; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool Load(const file_system::DirectoryPtr& directory);
    void Store(const file_system::DirectoryPtr& directory);
    bool IsValid() const { return mMinNumber <= mMaxNumber; }
private:
    volatile uint64_t mMinNumber;
    volatile uint64_t mMaxNumber;
private:
    friend class RangeInfoTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeInfo);

inline void RangeInfo::Update(uint64_t value)
{
    if (value < mMinNumber)
    {
        mMinNumber = value;
    }
    if (value > mMaxNumber)
    {
        mMaxNumber = value;
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TIME_RANGE_INFO_H
