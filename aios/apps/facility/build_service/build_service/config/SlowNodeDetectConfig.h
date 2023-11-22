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

#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"

namespace build_service { namespace config {

#define BACKUP_NODE_HANDLE_STRATEGY "backup_node"
#define MIGRATE_NODE_HANDLE_STRATEGY "migrate_node"
#define OLD_SLOW_NODE_DETECT_STRATEGY "old"
#define NEW_SLOW_NODE_DETECT_STRATEGY "new"

class SlowNodeDetectConfig : public autil::legacy::Jsonizable
{
public:
    SlowNodeDetectConfig();
    ~SlowNodeDetectConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    int64_t slowTimeThreshold;    // in second
    int64_t notWorkTimeThreshold; // in second
    int64_t maxKillTimesLimit;
    bool enableSlowDetect;
    bool enableBuilderSlowDetect;
    bool enableMergerSlowDetect;
    bool enableProcessorSlowDetect;
    bool enableTaskSlowDetect;
    bool enableHandleStrategyInterval;
    bool isTestMode;
    float slowNodeSampleRatio;
    float longTailNodeSampleRatio; // trigger contidition, if more than this ratio of nodes have been finished.
    float longTailNodeMultiplier;
    int64_t minimumSampleTime; // in second
    int64_t detectInterval;    // in second
    float slowQpsJudgeRatio;
    int64_t restartCountThreshold;
    int64_t killTimesResetWindow;
    int64_t backupNodeCountLimit;
    int64_t cpuSpeedFixedThreshold;
    std::string slowNodeHandleStrategy;
    std::string slowNodeDetectStrategy;
    bool enableHighQualitySlot;
    bool enableNetworkSpeedStrategy;
    bool enableDetectSlowByLocator;
    uint64_t networkSpeedThreshold; // Byte per second
    float cpuSpeedMinThresholdRatio;
    float cpuSpeedMaxThresholdRatio;
    bool enableBrutalStrategy;
    float brutalStrategyTriggerRatio;
    float brutalSlowCpuSpeedJudgeRatio;
    std::map<std::string, std::vector<std::string>> excludeKeywords;
    std::string locatorCheckCondition;
    int64_t locatorCheckGap;
    uint64_t locatorDetectMinServiceRatio;

    static const int64_t DEFAULT_SLOW_TIME_THRESHOLD;
    static const int64_t DEFAULT_NOT_WORK_TIME_THRESHOLD;
    static const int64_t DEFAULT_MAX_KILL_TIMES_LIMIT;
    static const float DEFAULT_SLOW_NODE_SAMPLE_RATIO;
    static const float DEFAULT_SLOW_QPS_JUDGE_RATIO;
    static const float DEFAULT_CPU_SPEED_MIN_THRESHOLD_RATIO;
    static const float DEFAULT_CPU_SPEED_MAX_THRESHOLD_RATIO;
    static const float DEFAULT_BRUTAL_STRATEGY_TRIGGER_RATIO;
    static const float DEFAULT_BRUTAL_STRATEGY_SLOW_CPU_JUDGE_RATIO;
    static const float DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO;
    static const float DEFAULT_LONG_TAIL_NODE_MULTIPLIER;
    static const int64_t DEFAULT_SAMPLE_TIME;
    static const int64_t DEFAULT_DETECT_INTERVAL;
    static const int64_t DEFAULT_RESTART_COUNT_THRESHOLD;
    static const int64_t DEFAULT_KILL_TIME_RESET_WINDOW;
    static const int64_t DEFAULT_BACKUP_NODE_COUNT_LIMIT;
    static const std::string DEFAULT_SLOW_NODE_HANDLE_STRATEGY;
    static const std::string DEFAULT_SLOW_NODE_DETECT_STRATEGY;
    static const uint64_t DEFAULT_NETWORK_SPEED_THRESHOLD;
    static const uint64_t DEFAULT_LOCATOR_DETECT_MIN_SERVICE_RATIO;
};

BS_TYPEDEF_PTR(SlowNodeDetectConfig);

}} // namespace build_service::config
