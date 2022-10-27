#ifndef ISEARCH_BS_BUILDSPEEDLIMITER_H
#define ISEARCH_BS_BUILDSPEEDLIMITER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace builder {

class BuildSpeedLimiter
{
private:
    static const int64_t ONE_SECOND = 1LL * 1000 * 1000;

public:
    BuildSpeedLimiter(const std::string &buildIdStr = "");
    ~BuildSpeedLimiter();

private:
    BuildSpeedLimiter(const BuildSpeedLimiter &);
    BuildSpeedLimiter& operator=(const BuildSpeedLimiter &);

public:
    void setTargetQps(int32_t sleepPerDoc, uint32_t buildQps);
    void enableLimiter() { _enableLimiter = true; }
    int32_t getTargetQps() const { return _targetQps; };
    void limitSpeed();

private:
    std::string _buildIdStr;
    uint32_t _targetQps;
    uint32_t _counter;
    int64_t _lastCheckTime;
    int32_t _sleepPerDoc;
    bool _enableLimiter; // enable only when realtime

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildSpeedLimiter);

}
}

#endif //ISEARCH_BS_BUILDSPEEDLIMITER_H
