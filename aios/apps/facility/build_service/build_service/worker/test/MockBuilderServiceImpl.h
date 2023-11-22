#pragma once

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/BuilderServiceImpl.h"
#include "build_service/worker/ServiceWorker.h"
#include "indexlib/util/EpochIdUtil.h"

namespace build_service { namespace worker {

class MockBuilderServiceImpl : public BuilderServiceImpl
{
public:
    MockBuilderServiceImpl(const proto::PartitionId& pid, const std::string& appZkRoot = "",
                           const std::string& adminServiceName = "")
        : BuilderServiceImpl(pid, NULL, proto::LongAddress(), appZkRoot, adminServiceName,
                             indexlib::util::EpochIdUtil::TEST_GenerateEpochId())
    {
    }
    ~MockBuilderServiceImpl() {}

private:
    MOCK_METHOD3(startBuildFlow, void(const config::ResourceReaderPtr&, const proto::PartitionId&, KeyValueMap&));
    MOCK_METHOD2(doHandleTargetState, void(const std::string&, bool));
    MOCK_METHOD1(doGetResourceKeeperMap, void(common::ResourceKeeperMap& usingKeepers));
    MOCK_METHOD1(needSuspendTask, bool(const std::string&));
    MOCK_METHOD1(getCurrentState, void(std::string&));
    MOCK_METHOD3(createFlowFactory, workflow::FlowFactory*(const config::ResourceReaderPtr& resourceReader,
                                                           const proto::PartitionId& pid, KeyValueMap& kvMap));
    MOCK_METHOD0(createBuildFlow, workflow::BuildFlow*());

    std::string getEpochId() const { return _epochId; }

private:
    ServiceWorker _worker;
};

typedef ::testing::StrictMock<MockBuilderServiceImpl> StrictMockBuilderServiceImpl;
typedef ::testing::NiceMock<MockBuilderServiceImpl> NiceMockBuilderServiceImpl;

}} // namespace build_service::worker
