/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

#include <sstream>

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BuildResourceMetrics);
AUTIL_LOG_SETUP(indexlib.util, MetricsSampler);

BuildResourceMetricsNode::BuildResourceMetricsNode(BuildResourceMetrics* parent, int nodeId)
    : _parent(parent)
    , _nodeId(nodeId)
{
    assert(parent);
    _metricsValues.resize(parent->GetMetricsSize());
}

BuildResourceMetricsNode::~BuildResourceMetricsNode() {}

BuildResourceMetrics::BuildResourceMetrics() {}

BuildResourceMetrics::~BuildResourceMetrics()
{
    for (size_t i = 0; i < _metricsNodes.size(); ++i) {
        DELETE_AND_SET_NULL(_metricsNodes[i]);
    }

    for (size_t i = 0; i < _metricsSamplers.size(); ++i) {
        DELETE_AND_SET_NULL(_metricsSamplers[i]);
    }
}

BuildResourceMetricsNode* BuildResourceMetrics::AllocateNode()
{
    ScopedLock lock(_mutex);
    BuildResourceMetricsNode* node = new BuildResourceMetricsNode(this, _metricsNodes.size());
    _metricsNodes.push_back(node);
    return node;
}

bool BuildResourceMetrics::RegisterMetrics(BuildResourceMetricsType bmt, BuildResourceSamplerType bst,
                                           const std::string& samplerName)
{
    ScopedLock lock(_mutex);
    if (!_metricsNodes.empty()) {
        AUTIL_LOG(ERROR, "cannot register metrics after call AllocateNode");
        return false;
    }

    if (bmt >= _metricsSamplers.size()) {
        _metricsSamplers.resize(bmt + 1, NULL);
    }

    if (_metricsSamplers[bmt]) {
        AUTIL_LOG(ERROR, "BuildResourceMetricsType:%d is already registered.", bmt);
        return false;
    }
    _metricsSamplers[bmt] = new MetricsSampler(bst, samplerName);
    return true;
}

string BuildResourceMetrics::GetMetricDetail(const string& metricsOwner) const
{
    stringstream ss;
    ss << metricsOwner << " build resource metrics: ";

    for (size_t i = 0; i < _metricsSamplers.size(); ++i) {
        if (_metricsSamplers[i]) {
            ss << _metricsSamplers[i]->GetName() << ": " << _metricsSamplers[i]->GetValue() << "; ";
        }
    }
    ss << endl;

    for (size_t i = 0; i < _metricsNodes.size(); ++i) {
        ss << "BuildResourceMetricsNode " << _metricsNodes[i]->GetNodeId() << ":";
        for (size_t j = 0; j < _metricsSamplers.size(); ++j) {
            if (_metricsSamplers[j]) {
                ss << _metricsSamplers[j]->GetName() << ":" << _metricsNodes[i]->GetValue(j) << ";";
            }
        }
        ss << endl;
    }
    return ss.str();
}

void BuildResourceMetrics::Print(const string& metricsOwner) const
{
    string detailStr = GetMetricDetail(metricsOwner);
    AUTIL_LOG(DEBUG, "%s", detailStr.c_str());
}
}} // namespace indexlib::util
