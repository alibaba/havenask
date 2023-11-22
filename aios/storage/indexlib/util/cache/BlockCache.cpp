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
#include "indexlib/util/cache/BlockCache.h"

#include "autil/EnvUtil.h"
#include "indexlib/util/cache/BlockAllocator.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BlockCache);

BlockCache::BlockCache() noexcept : _blockSize(0), _iOBatchSize(0)
{
    if (autil::EnvUtil::getEnv("INDEXLIB_REPORT_HEAVY_COST_METRIC", false) ||
        autil::EnvUtil::getEnv("INDEXLIB_REPORT_LIGHT_COST_METRIC", false)) {
        _blockCacheHitTagReporter.reset(new QpsTaggedMetricReporterGroup);
        _blockCacheMissTagReporter.reset(new QpsTaggedMetricReporterGroup);
        _blockCacheReadCountTagReporter.reset(new QpsTaggedMetricReporterGroup);
        _blockCacheReadSizeTagReporter.reset(new QpsTaggedMetricReporterGroup);
        _blockCacheReadLatencyTagReporter.reset(new InputTaggedMetricReporterGroup);
    }
}

BlockCache::~BlockCache() noexcept {}

bool BlockCache::Init(const BlockCacheOption& option)
{
    _blockSize = option.blockSize;
    _iOBatchSize = option.ioBatchSize;
    _memorySize = option.memorySize;
    if (_blockSize == 0) {
        AUTIL_LOG(ERROR, "blockSize is 0");
        return false;
    }

    _blockAllocator.reset(new BlockAllocator(_memorySize, _blockSize));
    return DoInit(option);
};

void BlockCache::RegisterMetrics(const util::MetricProviderPtr& metricProvider, const std::string& prefix,
                                 const kmonitor::MetricsTags& metricsTags)
{
    string urlPrefix = prefix;
    _metricsTags = metricsTags;

    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheHitRatio, urlPrefix + "/BlockCacheHitRatio", kmonitor::GAUGE, "%");
    IE_INIT_LOCAL_INPUT_METRIC(_hitRatioReporter, BlockCacheHitRatio);
    // HIT
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheHitQps, urlPrefix + "/BlockCacheHitQps", kmonitor::QPS, "count");
    // MIS
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheMissQps, urlPrefix + "/BlockCacheMissQps", kmonitor::QPS,
                         "count"); // BlockCacheReadQps
    IE_INIT_METRIC_GROUP(metricProvider, BlockCachePrefetchQps, urlPrefix + "/BlockCachePrefetchQps", kmonitor::QPS,
                         "count"); // BlockCacheReadQps
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadLatency, urlPrefix + "/BlockCacheReadLatency", kmonitor::GAUGE,
                         "us");
    IE_INIT_LOCAL_INPUT_METRIC(_readLatencyReporter, BlockCacheReadLatency);

    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadCount, urlPrefix + "/BlockCacheReadCount", kmonitor::GAUGE,
                         "count");

    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadOneBlockQps, urlPrefix + "/BlockCacheOneBlockQps", kmonitor::QPS,
                         "count");
    IE_INIT_METRIC_GROUP(metricProvider, BlockCacheReadMultiBlockQps, urlPrefix + "/BlockCacheMultiBlockQps",
                         kmonitor::QPS, "count");

    _blockCacheHitReporter.Init(mBlockCacheHitQpsMetric);
    _blockCacheMissReporter.Init(mBlockCacheMissQpsMetric);
    _blockCachePrefetchReporter.Init(mBlockCachePrefetchQpsMetric);

    if (_blockCacheHitTagReporter) {
        vector<string> limitedTagKeys;
        if (autil::EnvUtil::getEnv("INDEXLIB_REPORT_LIGHT_COST_METRIC", false)) {
            limitedTagKeys = {"identifier", "data_type", "file_name"};
        }
        _blockCacheHitTagReporter->Init(metricProvider, urlPrefix + "BlockCacheHitQpsWithTag", limitedTagKeys);
        assert(_blockCacheMissTagReporter);
        _blockCacheMissTagReporter->Init(metricProvider, urlPrefix + "BlockCacheMissQpsWithTag", limitedTagKeys);
        assert(_blockCacheReadCountTagReporter);
        _blockCacheReadCountTagReporter->Init(metricProvider, urlPrefix + "BlockCacheReadBlockCountWithTag",
                                              limitedTagKeys);
        assert(_blockCacheReadSizeTagReporter);
        _blockCacheReadSizeTagReporter->Init(metricProvider, urlPrefix + "BlockCacheReadSizeWithTag", limitedTagKeys);
        assert(_blockCacheReadLatencyTagReporter);
        _blockCacheReadLatencyTagReporter->Init(metricProvider, urlPrefix + "BlockCacheReadLatencyWithTag",
                                                limitedTagKeys);
    }

    _hitRatioReporter.MergeTags(metricsTags);
    _readLatencyReporter.MergeTags(metricsTags);
    _blockCacheHitReporter.MergeTags(metricsTags);
    _blockCacheMissReporter.MergeTags(metricsTags);
    _blockCachePrefetchReporter.MergeTags(metricsTags);
}

BlockCache::TaggedMetricReporter BlockCache::DeclareTaggedMetricReporter(const map<string, string>& tagMap) noexcept
{
    TaggedMetricReporter ret;
    if (_blockCacheHitTagReporter) {
        ret.hitQps = _blockCacheHitTagReporter->DeclareMetricReporter(tagMap);
    }
    if (_blockCacheMissTagReporter) {
        ret.missQps = _blockCacheMissTagReporter->DeclareMetricReporter(tagMap);
    }
    if (_blockCacheReadCountTagReporter) {
        ret.readCount = _blockCacheReadCountTagReporter->DeclareMetricReporter(tagMap);
    }
    if (_blockCacheReadSizeTagReporter) {
        ret.readSize = _blockCacheReadSizeTagReporter->DeclareMetricReporter(tagMap);
    }
    if (_blockCacheReadLatencyTagReporter) {
        ret.readLatency = _blockCacheReadLatencyTagReporter->DeclareMetricReporter(tagMap);
    }
    return ret;
}

bool BlockCache::ExtractCacheParam(const BlockCacheOption& cacheOption, int32_t& shardBitsNum,
                                   float& lruHighPriorityRatio, float& lruLowPriorityRatio) const
{
    string shardBitsNumStr =
        GetValueFromKeyValueMap(cacheOption.cacheParams, "num_shard_bits", to_string(DEFAULT_SHARED_BITS_NUM));
    if (!autil::StringUtil::fromString(shardBitsNumStr, shardBitsNum) || shardBitsNum < 0 || shardBitsNum >= 20) {
        AUTIL_LOG(ERROR, "parse block cache param failed,  num_shard_bits [%s] should be integer between [0, 20]",
                  shardBitsNumStr.c_str());
        return false;
    }

    string highPriorityRatioStr =
        GetValueFromKeyValueMap(cacheOption.cacheParams, "lru_high_priority_ratio", string("0"));
    if (!autil::StringUtil::fromString(highPriorityRatioStr, lruHighPriorityRatio) || lruHighPriorityRatio < 0.0 ||
        lruHighPriorityRatio > 1.0) {
        AUTIL_LOG(ERROR,
                  "parse block cache param failed,  lru_high_priority_ratio [%s] should be float between [0.0, 1.0]",
                  highPriorityRatioStr.c_str());
        return false;
    }

    float defaultLowPriorityRatio = 1.0 - lruHighPriorityRatio;
    string lowPriorityRatioStr =
        GetValueFromKeyValueMap(cacheOption.cacheParams, "lru_low_priority_ratio", to_string(defaultLowPriorityRatio));
    if (!autil::StringUtil::fromString(lowPriorityRatioStr, lruLowPriorityRatio) || lruLowPriorityRatio < 0.0 ||
        lruLowPriorityRatio > 1.0) {
        AUTIL_LOG(ERROR,
                  "parse block cache param failed,  lru_low_priority_ratio [%s] should be float between [0.0, 1.0]",
                  lowPriorityRatioStr.c_str());
        return false;
    }
    // add a very small bias to avoid accuracy error
    if (lruLowPriorityRatio + lruHighPriorityRatio > 1.0 + 0.000001) {
        AUTIL_LOG(
            ERROR,
            "parse block cache param failed,  lru_low_priority_ratio [%s] + lru_lru_high_priority_ratio [%s] should be float between [0.0, 1.0]\
                    \n If addition of the two numbers exactly equal to 1.0 from human view, this problem may be caused by accuracy error.",
            lowPriorityRatioStr.c_str(), highPriorityRatioStr.c_str());
        return false;
    }

    return true;
}

}} // namespace indexlib::util
