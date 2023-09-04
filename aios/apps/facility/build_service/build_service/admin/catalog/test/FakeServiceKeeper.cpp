#include "build_service/admin/catalog/test/FakeServiceKeeper.h"

namespace build_service::admin {

proto::StartBuildRequest gTestStartBuildRequest;
proto::StartBuildResponse gTestStartBuildResponse;
proto::StopBuildRequest gTestStopBuildRequest;
proto::InformResponse gTestStopBuildResponse;
proto::UpdateConfigRequest gTestUpdateConfigRequest;
proto::InformResponse gTestUpdateConfigResponse;

void ServiceKeeper::startBuild(const proto::StartBuildRequest* request, proto::StartBuildResponse* response)
{
    gTestStartBuildRequest = *request;
    *response = gTestStartBuildResponse;
}

void ServiceKeeper::stopBuild(const proto::StopBuildRequest* request, proto::InformResponse* response)
{
    gTestStopBuildRequest = *request;
    *response = gTestStopBuildResponse;
}

void ServiceKeeper::updateConfig(const proto::UpdateConfigRequest* request, proto::InformResponse* response)
{
    gTestUpdateConfigRequest = *request;
    *response = gTestUpdateConfigResponse;
}

} // namespace build_service::admin
