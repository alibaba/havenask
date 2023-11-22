#pragma once

#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

class ProtoTestHelper
{
private:
    ProtoTestHelper();
    ~ProtoTestHelper();
    ProtoTestHelper(const ProtoTestHelper&);
    ProtoTestHelper& operator=(const ProtoTestHelper&);

public:
    static BuilderTarget createBuilderTarget(const std::string& configPath = "/path/to/config",
                                             int64_t startFromCheckpoint = 0, int64_t stopToCheckpoint = 0);

    static BuilderCurrent createBuilderCurrent(const BuilderTarget& target, const std::string& address = "");

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProtoTestHelper);

}} // namespace build_service::proto
