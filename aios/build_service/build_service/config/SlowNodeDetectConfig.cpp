#include "build_service/config/SlowNodeDetectConfig.h"

using namespace std;

namespace build_service {
namespace config {

const int64_t SlowNodeDetectConfig::DEFAULT_SLOW_TIME_THRESHOLD = 10 * 60;
const int64_t SlowNodeDetectConfig::DEFAULT_MAX_KILL_TIMES_LIMIT = 2;
const float SlowNodeDetectConfig::DEFAULT_SLOW_NODE_SAMPLE_RATIO = 0.9;    
const float SlowNodeDetectConfig::DEFAULT_SLOW_QPS_JUDGE_RATIO = 0.8;
const float SlowNodeDetectConfig::DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO = 0.7;
const int64_t SlowNodeDetectConfig::DEFAULT_SAMPLE_TIME = 10 * 60; 
const int64_t SlowNodeDetectConfig::DEFAULT_DETECT_INTERVAL = 5;
const int64_t SlowNodeDetectConfig::DEFAULT_RESTART_COUNT_THRESHOLD = 5;
    
SlowNodeDetectConfig::SlowNodeDetectConfig()
    : slowTimeThreshold(DEFAULT_SLOW_TIME_THRESHOLD) //10min
    , maxKillTimesLimit(DEFAULT_MAX_KILL_TIMES_LIMIT)
    , enableSlowDetect(false)
    , slowNodeSampleRatio(DEFAULT_SLOW_NODE_SAMPLE_RATIO)
    , longTailNodeSampleRatio(DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO)
    , minimumSampleTime(DEFAULT_SAMPLE_TIME)
    , detectInterval(DEFAULT_DETECT_INTERVAL)
    , slowQpsJudgeRatio(DEFAULT_SLOW_QPS_JUDGE_RATIO)
    , restartCountThreshold(DEFAULT_RESTART_COUNT_THRESHOLD)
{
}

SlowNodeDetectConfig::~SlowNodeDetectConfig() {
}

void SlowNodeDetectConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("slow_time_threshold", slowTimeThreshold, slowTimeThreshold);
    json.Jsonize("max_kill_times", maxKillTimesLimit, maxKillTimesLimit);
    json.Jsonize("enable_slow_node_detect", enableSlowDetect, enableSlowDetect);
    json.Jsonize("slow_node_sample_ratio", slowNodeSampleRatio, slowNodeSampleRatio);
    json.Jsonize("long_tail_node_sample_ratio", longTailNodeSampleRatio, longTailNodeSampleRatio);
    json.Jsonize("minimum_sample_time", minimumSampleTime, minimumSampleTime);
    json.Jsonize("detect_interval", detectInterval, detectInterval);
    json.Jsonize("slow_qps_judge_ratio", slowQpsJudgeRatio, slowQpsJudgeRatio);
    json.Jsonize("restart_count_threshold", restartCountThreshold, restartCountThreshold); 
}
}
}
