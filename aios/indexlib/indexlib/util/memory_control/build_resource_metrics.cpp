#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BuildResourceMetrics);
IE_LOG_SETUP(util, MetricsSampler);


BuildResourceMetricsNode::BuildResourceMetricsNode(BuildResourceMetrics* parent, int nodeId)
    : mParent(parent)
    , mNodeId(nodeId)
{
    assert(parent);
    mMetricsValues.resize(parent->GetMetricsSize());
}

BuildResourceMetricsNode::~BuildResourceMetricsNode()
{
    
}

BuildResourceMetrics::BuildResourceMetrics()
{
}

BuildResourceMetrics::~BuildResourceMetrics()
{
    for (size_t i = 0; i < mMetricsNodes.size(); ++i)
    {
        DELETE_AND_SET_NULL(mMetricsNodes[i]);
    }
        
    for (size_t i = 0; i < mMetricsSamplers.size(); ++i)
    {
        DELETE_AND_SET_NULL(mMetricsSamplers[i]);
    }        
}

BuildResourceMetricsNode* BuildResourceMetrics::AllocateNode()
{
    ScopedLock lock(mMutex);
    BuildResourceMetricsNode* node = new BuildResourceMetricsNode(
            this, mMetricsNodes.size());
    mMetricsNodes.push_back(node);
    return node;
}

bool BuildResourceMetrics::RegisterMetrics(
        BuildResourceMetricsType bmt, BuildResourceSamplerType bst,
        const std::string& samplerName)
{
    ScopedLock lock(mMutex);    
    if (!mMetricsNodes.empty())
    {
        IE_LOG(ERROR, "cannot register metrics after call AllocateNode");
        return false;
    }

    if (bmt >= mMetricsSamplers.size())
    {
        mMetricsSamplers.resize(bmt + 1, NULL);
    }

    if (mMetricsSamplers[bmt])
    {
        IE_LOG(ERROR, "BuildResourceMetricsType:%d is already registered.", bmt);
        return false;
    }
    mMetricsSamplers[bmt] = new MetricsSampler(bst, samplerName);
    return true;
}

string BuildResourceMetrics::GetMetricDetail(const string& metricsOwner) const
{
    stringstream ss;
    ss << metricsOwner << " build resource metrics: ";
    
    for (size_t i = 0; i < mMetricsSamplers.size(); ++i)
    {
        if (mMetricsSamplers[i])
        {
            ss << mMetricsSamplers[i]->GetName() << ": " <<
                mMetricsSamplers[i]->GetValue() << "; ";
        }
    }
    ss << endl;

    for (size_t i = 0; i < mMetricsNodes.size(); ++i)
    {
        ss << "BuildResourceMetricsNode " << mMetricsNodes[i]->GetNodeId() << ":";
        for (size_t j = 0; j < mMetricsSamplers.size(); ++j)
        {
            if (mMetricsSamplers[j])
            {
                ss << mMetricsSamplers[j]->GetName() << ":"
                   << mMetricsNodes[i]->GetValue(j) << ";";
            }
        }
        ss << endl;
    }
    return ss.str();
}

void BuildResourceMetrics::Print(const string& metricsOwner) const
{
    string detailStr = GetMetricDetail(metricsOwner);
    IE_LOG(DEBUG, "%s", detailStr.c_str());
}

IE_NAMESPACE_END(util);

