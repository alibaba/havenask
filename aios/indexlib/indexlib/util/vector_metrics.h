#ifndef __INDEXLIB_VECTOR_METRICS_H
#define __INDEXLIB_VECTOR_METRICS_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/metrics_base.h"
#include "indexlib/util/guard_lock.h"
#include <tr1/memory>
#include <vector>

IE_NAMESPACE_BEGIN(util);

template <typename T, typename Lock>
class VectorMetrics : public MetricsBase
{
public:
    typedef Lock LockType;
    typedef T ElemType;
    typedef std::vector<ElemType> ValueVector;

public:
    VectorMetrics(const std::string& metricsName) : MetricsBase(metricsName) {}
    ~VectorMetrics() {}

public:
    inline void PushBack(const ElemType& value);

    inline void Reserve(size_t size);
    inline size_t Size();
    inline const ElemType& GetValue(size_t idx);

    inline int64_t GetValue() { return 0;}
    inline int64_t GetCount();

    void Reset() { mValues.clear(); }

    const ValueVector& GetVector() const { return mValues;}

private:
    ValueVector mValues;
    LockType mLock;
};

typedef VectorMetrics<std::pair<std::string, int64_t>, autil::RecursiveThreadMutex> PairedVectorMetrics;
typedef std::tr1::shared_ptr<PairedVectorMetrics> PairedVectorMetricsPtr;

typedef VectorMetrics<std::pair<std::string, int64_t>, NullLock> UnsafePairedVectorMetrics;
typedef std::tr1::shared_ptr<UnsafePairedVectorMetrics> UnsafePairedVectorMetricsPtr;

typedef VectorMetrics<std::string, autil::RecursiveThreadMutex> StrVectorMetrics;
typedef std::tr1::shared_ptr<StrVectorMetrics> StrVectorMetricsPtr;

typedef VectorMetrics<std::string, NullLock> UnsafeStrVectorMetrics;
typedef std::tr1::shared_ptr<UnsafeStrVectorMetrics> UnsafeStrVectorMetricsPtr;

typedef VectorMetrics<int64_t, autil::RecursiveThreadMutex> Int64VectorMetrics;
typedef std::tr1::shared_ptr<Int64VectorMetrics> Int64VectorMetricsPtr;

typedef VectorMetrics<int64_t, NullLock> UnsafeInt64VectorMetrics;
typedef std::tr1::shared_ptr<UnsafeInt64VectorMetrics> UnsafeInt64VectorMetricsPtr;

typedef VectorMetrics<int32_t, autil::RecursiveThreadMutex> Int32VectorMetrics;
typedef std::tr1::shared_ptr<Int32VectorMetrics> Int32VectorMetricsPtr;

typedef VectorMetrics<int32_t, NullLock> UnsafeInt32VectorMetrics;
typedef std::tr1::shared_ptr<UnsafeInt32VectorMetrics> UnsafeInt32VectorMetricsPtr;

///////////////////////////////////////////////////////
//
template <typename T, typename Lock>
inline void VectorMetrics<T, Lock>::PushBack(const T& value)
{
    GuardLock<Lock> lock(mLock);
    mValues.push_back(value);
}

template <typename T, typename Lock>
inline void VectorMetrics<T, Lock>::Reserve(size_t size)
{
    GuardLock<Lock> lock(mLock);
    mValues.reserve(size);
}

template <typename T, typename Lock>
inline size_t VectorMetrics<T, Lock>::Size()
{
    GuardLock<Lock> lock(mLock);
    return mValues.size();
}

template <typename T, typename Lock>
inline const T& VectorMetrics<T, Lock>::GetValue(size_t idx)
{
    GuardLock<Lock> lock(mLock);
    return mValues[idx];
}

template <typename T, typename Lock>    
inline int64_t VectorMetrics<T, Lock>::GetCount() 
{
    GuardLock<Lock> lock(mLock);
    return mValues.size();
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_VECTOR_METRICS_H
