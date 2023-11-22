#pragma once

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/BuildJobImpl.h"
#include "build_service/workflow/BuildFlow.h"

namespace build_service { namespace worker {

class MockBuildJobImpl : public BuildJobImpl
{
public:
    MockBuildJobImpl(const proto::PartitionId& pid,
                     indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr())
        : BuildJobImpl(pid, metricProvider, proto::LongAddress())
    {
        ON_CALL(*this, createBuildFlow()).WillByDefault(Invoke(std::bind(&MockBuildJobImpl::doCreateBuildFlow, this)));
    }
    ~MockBuildJobImpl() {}

protected:
    MOCK_METHOD1(build, bool(const proto::JobTarget&));
    MOCK_METHOD0(createBuildFlow, workflow::BuildFlow*());

private:
    workflow::BuildFlow* doCreateBuildFlow() { return BuildJobImpl::createBuildFlow(); }
};

typedef ::testing::StrictMock<MockBuildJobImpl> StrictMockBuildJobImpl;
typedef ::testing::NiceMock<MockBuildJobImpl> NiceMockBuildJobImpl;

}} // namespace build_service::worker
