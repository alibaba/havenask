#pragma once

#include "suez/heartbeat/HeartbeatTarget.h"

namespace suez {

class HeartbeatTargetUtil {
public:
    static HeartbeatTarget prepareTarget(const std::vector<std::pair<std::string, std::string>> &strVec,
                                         bool cleanDisk = false,
                                         bool allowPreload = false);
};

} // namespace suez
