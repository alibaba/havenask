#ifndef ISEARCH_BS_MOCKRPCCHANNEL_H
#define ISEARCH_BS_MOCKRPCCHANNEL_H

#include "build_service/util/Log.h"
#include "build_service_tasks/channel/BsAdminChannel.h"
#include "build_service_tasks/channel/MadroxChannel.h"
#include "build_service_tasks/channel/Master.pb.h"
#include "build_service_tasks/test/unittest.h"

namespace build_service { namespace task_base {

class MockMadroxChannel : public MadroxChannel
{
public:
    MockMadroxChannel() : MadroxChannel("") {}
    ~MockMadroxChannel() = default;

public:
    MOCK_METHOD2(GetStatus, bool(const ::madrox::proto::GetStatusRequest&, ::madrox::proto::GetStatusResponse&));
    MOCK_METHOD2(UpdateRequest, bool(const ::madrox::proto::UpdateRequest&, ::madrox::proto::UpdateResponse&));
};

class MockBsAdminChannel : public BsAdminChannel
{
public:
    MockBsAdminChannel() : BsAdminChannel("") {}
    ~MockBsAdminChannel() = default;

public:
    MOCK_METHOD2(GetServiceInfo, bool(const proto::ServiceInfoRequest&, proto::ServiceInfoResponse&));
    MOCK_METHOD2(CreateSnapshot, bool(const proto::CreateSnapshotRequest&, proto::InformResponse&));
    MOCK_METHOD2(RemoveSnapshot, bool(const proto::RemoveSnapshotRequest&, proto::InformResponse&));
};

}} // namespace build_service::task_base

#endif // ISEARCH_BS_MOCKRPCCHANNEL_H
