#ifndef __INDEXLIB_LATENCY_STAT_H
#define __INDEXLIB_LATENCY_STAT_H

#include <atomic>
#include <cmath>

#include "indexlib/common_define.h"
#include "indexlib/misc/status.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(util);

class LatencyStat
{
public:
    LatencyStat(){Reset();}
    ~LatencyStat(){}

private:
    static const uint32_t INDEX_BITS_IN_GROUP = 6;
    static const uint32_t BUCKETS_IN_GROUP = (1 << INDEX_BITS_IN_GROUP);
    static const uint32_t GROUP_COUNT = 19;
    static const uint32_t BUCKET_COUNT = GROUP_COUNT * BUCKETS_IN_GROUP;

 // Eg. Let M (INDEX_BITS_IN_GROUP) be 6
 //      Error bound is 1/2^(6+1) = 0.0078125 (< 1%)
 //
 //	Group  MSB	#discarded	range of		#buckets
 //			    error_bits	value
 //	----------------------------------------------------------------
 //	0*	   0~5	0		   [0,63]			   64
 //	1*	   6	0		   [64,127]		       64
 //	2	   7	1		   [128,255]		   64
 //	3	   8	2		   [256,511]		   64
 //	4	   9	3		   [512,1023]		   64
 //	...	   ...	...		   [...,...]		   ...
 //	18	   23	17         [8388608,+inf]**	   64
 //
 //  * Special cases: when n < (M-1) or when n == (M-1), in both cases,
 //    the value cannot be rounded off. Use all bits of the sample as
 //    index.
 //
 //  ** If a sample's MSB is greater than 23, it will be counted as 23.

public:
    static uint32_t LatencyToIdx(uint32_t val)
    {
        uint32_t msb, errorBits, base, offset, idx;
        // Find MSB starting from bit 0
        if (val == 0)
        {
            msb = 0;
        }
        else
        {
            msb = 32 - __builtin_clz(val) - 1;
        }
        // msb <= INDEX_BITS_IN_GROUP, means val <= 127, in Group 0 or 1
        // data are directly mapped (one value <---> one bucket)
        // Use all bits of the sample as index
        if (msb <= INDEX_BITS_IN_GROUP)
        {
            return val;
        }
        // Compute the number of error bits to discard
        errorBits = msb - INDEX_BITS_IN_GROUP;
        // Compute the number of buckets before the group
        base = (errorBits + 1) << INDEX_BITS_IN_GROUP;
        // Discard the error bits and apply the mask to find the
        // index for the buckets in the group
        offset = (BUCKETS_IN_GROUP - 1) & (val >> errorBits);
        // Make sure the index does not exceed (array size - 1)
        idx = (base + offset < BUCKET_COUNT) ?
              (base + offset) : (BUCKET_COUNT - 1);
        return idx;
    }
    static uint32_t IdxToLatency(uint32_t idx)
    {
        uint32_t errorBits, k, baseLow;
        assert(idx < BUCKET_COUNT);
        // idx <= 127, in Group 0 or 1
        // data are directly mapped (one value <---> one bucket)
        if (idx < (BUCKETS_IN_GROUP << 1))
        {
            return idx;
        }
        // Find the group and compute the minimum value of that group
        errorBits = (idx >> INDEX_BITS_IN_GROUP) - 1;
        baseLow = 1 << (errorBits + INDEX_BITS_IN_GROUP);
        // Find its bucket number of the group
        k = idx % BUCKETS_IN_GROUP;
        // Return the mean of the range of the bucket
        return baseLow + ((k + 0.5) * (1 << errorBits));
    }

public:
    void AddSample(uint32_t latency)
    {
        uint32_t idx = LatencyToIdx(latency);
        assert(idx < BUCKET_COUNT);
        // atomic increment, doesn't matter if sigma(mHistogram) = mDataCnt + 1
        mHistogram[idx]++;
        mDataCnt++;
    }

    void GetPercentile(const std::vector<float>& pList,
                       std::vector<uint32_t>& ansList)
    {
        uint32_t len = pList.size();
        assert(len > 0);
        ansList.resize(len);
        bool isLast = false;
        uint64_t sum = 0;
        uint32_t i = 0, j = 0;

        for (i = 0; i < BUCKET_COUNT && !isLast; ++i)
        {
            sum += mHistogram[i];
            while (sum >= std::ceil(pList[j] / 100.0 * mDataCnt))
            {
                assert(pList[j] > 0 && pList[j] <= 100.0);
                ansList[j] = IdxToLatency(i);
                ++j;
                if (j == len)
                {
                    isLast = true;
                    break;
                }
            }
        }
    }

    void Reset()
    {
        for (uint32_t i = 0; i < BUCKET_COUNT; ++i)
        {
            mHistogram[i] = 0;
        }
        mDataCnt = 0;
    }

private:
    std::atomic<uint64_t> mHistogram[BUCKET_COUNT];
    std::atomic<uint64_t> mDataCnt;

protected:
    friend class LatencyStatTest;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LATENCY_STAT_H
