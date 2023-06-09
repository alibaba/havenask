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
#pragma once

#include <atomic>
#include <memory>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace indexlib { namespace util {

enum BuildResourceMetricsType {
    BMT_CURRENT_MEMORY_USE = 0,
    // expanded memory use when dumping (not released in dump thread)
    BMT_DUMP_EXPAND_MEMORY_SIZE,
    // tempory memory use when dumping (will released in the dump thread)
    BMT_DUMP_TEMP_MEMORY_SIZE,
    BMT_DUMP_FILE_SIZE
};

enum BuildResourceSamplerType { BST_ACCUMULATE_SAMPLE = 0, BST_MAX_SAMPLE };

class MetricsSampler
{
public:
    MetricsSampler(BuildResourceSamplerType bst, const std::string& name)
        : _samplerType(bst)
        , _value(0)
        , _samplerName(name)
    {
    }

    ~MetricsSampler() {}

public:
    void Sample(int64_t oldValue, int64_t newValue)
    {
        if (oldValue == newValue) {
            return;
        }

        switch (_samplerType) {
        case BST_ACCUMULATE_SAMPLE:
            _value += (newValue - oldValue);
            break;
        case BST_MAX_SAMPLE:
            _value = std::max(int64_t(_value), newValue);
            break;
        default:
            AUTIL_LOG(ERROR, "undefined sampler type: %d", _samplerType);
            assert(false);
        }
    }

    int64_t GetValue() const { return _value; }

    const std::string& GetName() const { return _samplerName; }

private:
    BuildResourceSamplerType _samplerType;
    std::atomic_long _value;
    std::string _samplerName;

private:
    AUTIL_LOG_DECLARE();
};

class BuildResourceMetricsNode;
class BuildResourceMetrics
{
public:
    BuildResourceMetrics();
    ~BuildResourceMetrics();

private:
    BuildResourceMetrics(const BuildResourceMetrics& other);
    BuildResourceMetrics& operator=(const BuildResourceMetrics& other);

public:
    void Init()
    {
        RegisterMetrics(BMT_CURRENT_MEMORY_USE, BST_ACCUMULATE_SAMPLE, "CurrentMemoryUse");
        RegisterMetrics(BMT_DUMP_EXPAND_MEMORY_SIZE, BST_ACCUMULATE_SAMPLE, "DumpExpandMemoryUse");
        RegisterMetrics(BMT_DUMP_TEMP_MEMORY_SIZE, BST_MAX_SAMPLE, "DumpTempMemoryUse");
        RegisterMetrics(BMT_DUMP_FILE_SIZE, BST_ACCUMULATE_SAMPLE, "DumpFileSize");
    }

    bool RegisterMetrics(BuildResourceMetricsType bmt, BuildResourceSamplerType bst, const std::string& samplerName);

    int64_t GetValue(BuildResourceMetricsType bmt) const
    {
        MetricsSampler* sampler = GetSampler(bmt);
        return sampler ? sampler->GetValue() : -1;
    }

    MetricsSampler* GetSampler(BuildResourceMetricsType bmt) const
    {
        if (bmt >= _metricsSamplers.size()) {
            return NULL;
        }
        return _metricsSamplers[bmt];
    }

    BuildResourceMetricsNode* AllocateNode();

    size_t GetMetricsSize() const { return _metricsSamplers.size(); }

    std::string GetMetricDetail(const std::string& metricsOwner) const;
    void Print(const std::string& metricsOwner) const;

    size_t GetNodeCount() const { return _metricsNodes.size(); }

private:
    std::vector<MetricsSampler*> _metricsSamplers;
    std::vector<BuildResourceMetricsNode*> _metricsNodes;
    autil::ThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};

class BuildResourceMetricsNode
{
public:
    BuildResourceMetricsNode(BuildResourceMetrics* parent, int nodeId);
    ~BuildResourceMetricsNode();

public:
    bool Update(BuildResourceMetricsType bmt, int64_t newValue)
    {
        assert(_parent);
        MetricsSampler* sampler = _parent->GetSampler(bmt);
        if (!sampler) {
            return false;
        }
        sampler->Sample(_metricsValues[bmt], newValue);
        _metricsValues[bmt] = newValue;
        return true;
    }

    int GetNodeId() const { return _nodeId; }
    int64_t GetValue(size_t idx) const { return _metricsValues[idx]; }

private:
    std::vector<int64_t> _metricsValues;
    BuildResourceMetrics* _parent;
    int _nodeId;
};

typedef std::shared_ptr<BuildResourceMetrics> BuildResourceMetricsPtr;
}} // namespace indexlib::util
