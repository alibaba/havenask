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
#include "build_service/config/SlowNodeDetectConfig.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace config {

const int64_t SlowNodeDetectConfig::DEFAULT_SLOW_TIME_THRESHOLD = 10 * 60;
const int64_t SlowNodeDetectConfig::DEFAULT_NOT_WORK_TIME_THRESHOLD = 20 * 60;
const int64_t SlowNodeDetectConfig::DEFAULT_MAX_KILL_TIMES_LIMIT = 2;
const float SlowNodeDetectConfig::DEFAULT_SLOW_NODE_SAMPLE_RATIO = 0.9;
const float SlowNodeDetectConfig::DEFAULT_SLOW_QPS_JUDGE_RATIO = 0.8;
const float SlowNodeDetectConfig::DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO = 0.7;
const float SlowNodeDetectConfig::DEFAULT_LONG_TAIL_NODE_MULTIPLIER = 1.4;
const int64_t SlowNodeDetectConfig::DEFAULT_SAMPLE_TIME = 10 * 60;
const int64_t SlowNodeDetectConfig::DEFAULT_DETECT_INTERVAL = 5; // 5s
const int64_t SlowNodeDetectConfig::DEFAULT_RESTART_COUNT_THRESHOLD = 5;

const int64_t SlowNodeDetectConfig::DEFAULT_KILL_TIME_RESET_WINDOW = 2400; // 40 min
const int64_t SlowNodeDetectConfig::DEFAULT_BACKUP_NODE_COUNT_LIMIT = 2;
const uint64_t SlowNodeDetectConfig::DEFAULT_NETWORK_SPEED_THRESHOLD = 1200000000;  // Byte per second
const uint64_t SlowNodeDetectConfig::DEFAULT_LOCATOR_DETECT_MIN_SERVICE_RATIO = 90; // 90%

const string SlowNodeDetectConfig::DEFAULT_SLOW_NODE_HANDLE_STRATEGY = MIGRATE_NODE_HANDLE_STRATEGY;
const string SlowNodeDetectConfig::DEFAULT_SLOW_NODE_DETECT_STRATEGY = NEW_SLOW_NODE_DETECT_STRATEGY;

SlowNodeDetectConfig::SlowNodeDetectConfig()
    : slowTimeThreshold(DEFAULT_SLOW_TIME_THRESHOLD) // 10min
    , notWorkTimeThreshold(DEFAULT_NOT_WORK_TIME_THRESHOLD)
    , maxKillTimesLimit(DEFAULT_MAX_KILL_TIMES_LIMIT)
    , enableSlowDetect(false)
    , enableBuilderSlowDetect(true)
    , enableMergerSlowDetect(true)
    , enableProcessorSlowDetect(true)
    , enableTaskSlowDetect(true)
    , enableHandleStrategyInterval(true)
    , isTestMode(false)
    , slowNodeSampleRatio(DEFAULT_SLOW_NODE_SAMPLE_RATIO)
    , longTailNodeSampleRatio(DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO)
    , longTailNodeMultiplier(DEFAULT_LONG_TAIL_NODE_MULTIPLIER)
    , minimumSampleTime(DEFAULT_SAMPLE_TIME)
    , detectInterval(DEFAULT_DETECT_INTERVAL)
    , slowQpsJudgeRatio(DEFAULT_SLOW_QPS_JUDGE_RATIO)
    , restartCountThreshold(DEFAULT_RESTART_COUNT_THRESHOLD)
    , killTimesResetWindow(DEFAULT_KILL_TIME_RESET_WINDOW)
    , backupNodeCountLimit(DEFAULT_BACKUP_NODE_COUNT_LIMIT)
    , cpuSpeedFixedThreshold(-1)
    , slowNodeHandleStrategy(DEFAULT_SLOW_NODE_HANDLE_STRATEGY)
    , slowNodeDetectStrategy(NEW_SLOW_NODE_DETECT_STRATEGY)
    , enableHighQualitySlot(false)
    , enableNetworkSpeedStrategy(false)
    , enableDetectSlowByLocator(false)
    , networkSpeedThreshold(DEFAULT_NETWORK_SPEED_THRESHOLD)
    , locatorCheckGap(-1)
    , locatorDetectMinServiceRatio(DEFAULT_LOCATOR_DETECT_MIN_SERVICE_RATIO)
{
}

SlowNodeDetectConfig::~SlowNodeDetectConfig() {}

void SlowNodeDetectConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("slow_time_threshold", slowTimeThreshold, slowTimeThreshold);
    json.Jsonize("not_work_time_threshold", notWorkTimeThreshold, notWorkTimeThreshold);
    json.Jsonize("max_kill_times", maxKillTimesLimit, maxKillTimesLimit);
    json.Jsonize("enable_slow_node_detect", enableSlowDetect, enableSlowDetect);
    json.Jsonize("enable_builder_slow_node_detect", enableBuilderSlowDetect, enableBuilderSlowDetect);
    json.Jsonize("enable_merger_slow_node_detect", enableMergerSlowDetect, enableMergerSlowDetect);
    json.Jsonize("enable_processor_slow_node_detect", enableProcessorSlowDetect, enableProcessorSlowDetect);
    json.Jsonize("enable_task_slow_node_detect", enableTaskSlowDetect, enableTaskSlowDetect);
    json.Jsonize("enable_handle_strategy_interval", enableHandleStrategyInterval, enableHandleStrategyInterval);
    json.Jsonize("is_test_mode", isTestMode, isTestMode);
    json.Jsonize("slow_node_sample_ratio", slowNodeSampleRatio, slowNodeSampleRatio);
    json.Jsonize("long_tail_node_sample_ratio", longTailNodeSampleRatio, longTailNodeSampleRatio);
    json.Jsonize("long_tail_node_multiplier", longTailNodeMultiplier, longTailNodeMultiplier);
    json.Jsonize("minimum_sample_time", minimumSampleTime, minimumSampleTime);
    json.Jsonize("detect_interval", detectInterval, detectInterval);
    json.Jsonize("slow_qps_judge_ratio", slowQpsJudgeRatio, slowQpsJudgeRatio);
    json.Jsonize("restart_count_threshold", restartCountThreshold, restartCountThreshold);
    json.Jsonize("kill_time_reset_window", killTimesResetWindow, killTimesResetWindow);
    json.Jsonize("backup_node_count_limit", backupNodeCountLimit, backupNodeCountLimit);
    json.Jsonize("cpu_speed_fixed_threshold", cpuSpeedFixedThreshold, cpuSpeedFixedThreshold);
    json.Jsonize("slow_node_handle_strategy", slowNodeHandleStrategy, slowNodeHandleStrategy);
    json.Jsonize("slow_node_detect_strategy", slowNodeDetectStrategy, slowNodeDetectStrategy);
    json.Jsonize("enable_high_quality_slot", enableHighQualitySlot, enableHighQualitySlot);
    json.Jsonize("enable_network_speed_strategy", enableNetworkSpeedStrategy, enableNetworkSpeedStrategy);
    json.Jsonize("enable_detect_slow_by_locator", enableDetectSlowByLocator, enableDetectSlowByLocator);
    json.Jsonize("network_speed_threshold", networkSpeedThreshold, networkSpeedThreshold);
    json.Jsonize("exclude_rolename_keywords", excludeKeywords, excludeKeywords);
    json.Jsonize("locator_check_condition", locatorCheckCondition, locatorCheckCondition);
    json.Jsonize("locator_check_gap", locatorCheckGap, locatorCheckGap);
    json.Jsonize("locator_detect_min_service_ratio", locatorDetectMinServiceRatio, locatorDetectMinServiceRatio);
}

}} // namespace build_service::config
