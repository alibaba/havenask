#ifndef __INDEXLIB_PROGRESS_METRICS_H
#define __INDEXLIB_PROGRESS_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class ProgressMetrics
{
public:
    ProgressMetrics(double total)
        : mTotal(total)
        , mCurrentRatio(0.0f)
    {}
    
    ~ProgressMetrics() {}
    
public:
    void SetTotal(double total) { mTotal = total; }
    
    void UpdateCurrentRatio(double ratio)
    {
        assert(ratio <= 1.0f && ratio >= 0.0f);
        assert(ratio >= mCurrentRatio);
        mCurrentRatio = ratio;
    }
    
    void SetFinish() { UpdateCurrentRatio(1.0f); }

    double GetTotal() const {  return mTotal; }
    double GetCurrentRatio() const { return mCurrentRatio; }
    double GetCurrent() const { return mTotal * mCurrentRatio; }

    bool IsFinished() const { return mCurrentRatio >= 1.0f; }

private:
    double mTotal;
    double mCurrentRatio;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ProgressMetrics);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PROGRESS_METRICS_H
