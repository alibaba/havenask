#pragma once

#include "build_service/admin/ServiceKeeper.h"

namespace build_service::admin {
extern proto::StartBuildRequest gTestStartBuildRequest;
extern proto::StartBuildResponse gTestStartBuildResponse;
extern proto::StopBuildRequest gTestStopBuildRequest;
extern proto::InformResponse gTestStopBuildResponse;
extern proto::UpdateConfigRequest gTestUpdateConfigRequest;
extern proto::InformResponse gTestUpdateConfigResponse;

} // namespace build_service::admin
