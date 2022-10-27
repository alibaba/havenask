#ifndef __INDEXLIB_BUILD_RESOURCE_METRICS_H
#define __INDEXLIB_BUILD_RESOURCE_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/Lock.h>
#include <atomic>

IE_NAMESPACE_BEGIN(util);

enum BuildResourceMetricsType
{
    BMT_CURRENT_MEMORY_USE = 0,
    // expanded memory use when dumping (not released in dump thread)
    BMT_DUMP_EXPAND_MEMORY_SIZE, 
    // tempory memory use when dumping (will released in the dump thread)     
    BMT_DUMP_TEMP_MEMORY_SIZE,
    BMT_DUMP_FILE_SIZE
};

enum BuildResourceSamplerType
{
    BST_ACCUMULATE_SAMPLE = 0,
    BST_MAX_SAMPLE
};

class MetricsSampler
{
public: 
    MetricsSampler(BuildResourceSamplerType bst, const std::string& name)
        : mSamplerType(bst)
        , mValue(0)
        , mSamplerName(name)
    {}

    ~MetricsSampler() {}
    
public:
    void Sample(int64_t oldValue, int64_t newValue)
    {
        if (oldValue == newValue)
        {
            return;
        }
        
        switch(mSamplerType)
        {
        case BST_ACCUMULATE_SAMPLE:
            mValue += (newValue - oldValue);
            break;
        case BST_MAX_SAMPLE:
            mValue = std::max(int64_t(mValue), newValue);
            break;
        default:
            IE_LOG(ERROR, "undefined sampler type: %d", mSamplerType);
            assert(false);
        }
    }

    int64_t GetValue() const
    {
        return mValue;
    }

    const std::string& GetName() const
    {
        return mSamplerName;
    }

private:
    BuildResourceSamplerType mSamplerType;
    std::atomic_long mValue;
    std::string mSamplerName;
private:
    IE_LOG_DECLARE();    
};

class BuildResourceMetricsNode;
class BuildResourceMetrics
{
public:
    BuildResourceMetrics();
    ~BuildResourceMetrics();

private:
    BuildResourceMetrics(const BuildResourceMetrics& other);
    BuildResourceMetrics& operator = (const BuildResourceMetrics& other);
    
public:
    void Init()
    {
        RegisterMetrics(BMT_CURRENT_MEMORY_USE, BST_ACCUMULATE_SAMPLE, "CurrentMemoryUse");
        RegisterMetrics(BMT_DUMP_EXPAND_MEMORY_SIZE, BST_ACCUMULATE_SAMPLE, "DumpExpandMemoryUse");
        RegisterMetrics(BMT_DUMP_TEMP_MEMORY_SIZE, BST_MAX_SAMPLE, "DumpTempMemoryUse");
        RegisterMetrics(BMT_DUMP_FILE_SIZE, BST_ACCUMULATE_SAMPLE, "DumpFileSize"); 
    }
    
    bool RegisterMetrics(BuildResourceMetricsType bmt,
                         BuildResourceSamplerType bst,
                         const std::string& samplerName);

    int64_t GetValue(BuildResourceMetricsType bmt) const
    {
        MetricsSampler* sampler = GetSampler(bmt);
        return sampler ? sampler->GetValue() : -1;
    }

    MetricsSampler* GetSampler(BuildResourceMetricsType bmt) const
    {
        if (bmt >= mMetricsSamplers.size())
        {
            return NULL;
        }
        return mMetricsSamplers[bmt];
    }

    BuildResourceMetricsNode* AllocateNode();

    size_t GetMetricsSize() const
    {
        return mMetricsSamplers.size();
    }
    
    std::string GetMetricDetail(const std::string& metricsOwner) const;
    void Print(const std::string& metricsOwner) const;

    size_t GetNodeCount() const
    {
        return mMetricsNodes.size();
    }
    
private:
    std::vector<MetricsSampler*> mMetricsSamplers;
    std::vector<BuildResourceMetricsNode*> mMetricsNodes;
    autil::ThreadMutex mMutex;
private:
    IE_LOG_DECLARE();
};

class BuildResourceMetricsNode
{
public:
    BuildResourceMetricsNode(BuildResourceMetrics* parent, int nodeId);
    ~BuildResourceMetricsNode();
    
public:
    bool Update(BuildResourceMetricsType bmt, int64_t newValue)
    {
        assert(mParent);
        MetricsSampler* sampler = mParent->GetSampler(bmt);
        if (!sampler)
        {
            return false;
        }
        sampler->Sample(mMetricsValues[bmt], newValue);
        mMetricsValues[bmt] = newValue;
        return true;
    }

    int GetNodeId() const { return mNodeId; }
    int64_t GetValue(size_t idx) const
    {
        return mMetricsValues[idx];
    }
    
private:
    std::vector<int64_t> mMetricsValues;
    BuildResourceMetrics* mParent;
    int mNodeId;
};

DEFINE_SHARED_PTR(BuildResourceMetrics);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUILD_RESOURCE_METRICS_H
