#ifndef ISEARCH_BS_SLOWNODEDETECTCONFIG_H
#define ISEARCH_BS_SLOWNODEDETECTCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class SlowNodeDetectConfig : public autil::legacy::Jsonizable
{
public:
    SlowNodeDetectConfig();
    ~SlowNodeDetectConfig();
    
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    int64_t slowTimeThreshold; //second
    int64_t maxKillTimesLimit;
    bool enableSlowDetect;
    float slowNodeSampleRatio;
    float longTailNodeSampleRatio;
    int64_t minimumSampleTime; //second
    int64_t detectInterval; //second
    float slowQpsJudgeRatio;
    int64_t restartCountThreshold;
public:
    static const int64_t DEFAULT_SLOW_TIME_THRESHOLD;
    static const int64_t DEFAULT_MAX_KILL_TIMES_LIMIT;
    static const float DEFAULT_SLOW_NODE_SAMPLE_RATIO;
    static const float DEFAULT_SLOW_QPS_JUDGE_RATIO;
    static const float DEFAULT_LONG_TAIL_NODE_SAMPLE_RATIO;
    static const int64_t DEFAULT_SAMPLE_TIME;
    static const int64_t DEFAULT_DETECT_INTERVAL;
    static const int64_t DEFAULT_RESTART_COUNT_THRESHOLD;
};

BS_TYPEDEF_PTR(SlowNodeDetectConfig);

}
}

#endif //ISEARCH_BS_SLOWNODEDETECTCONFIG_H
