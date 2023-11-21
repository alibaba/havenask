#pragma once

#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/worker/ProcessorServiceImpl.h"
#include "build_service/workflow/BuildFlow.h"

namespace build_service { namespace worker {

class MockProcessorServiceImpl : public ProcessorServiceImpl
{
public:
    MockProcessorServiceImpl(const proto::PartitionId& pid)
        : ProcessorServiceImpl(pid, NULL, proto::LongAddress(), "path/to/zk/root", "")
    {
        ON_CALL(*this, createBuildFlow(_))
            .WillByDefault(
                Invoke(std::bind(&MockProcessorServiceImpl::doCreateBuildFlow, this, std::placeholders::_1)));
        ON_CALL(*this, startProcessFlow(_, _, _))
            .WillByDefault(Invoke(std::bind(&MockProcessorServiceImpl::doStartProcessFlow, this, std::placeholders::_1,
                                            std::placeholders::_2, std::placeholders::_3)));
    }
    ~MockProcessorServiceImpl() {}

private:
    MOCK_METHOD3(startProcessFlow, bool(const config::ResourceReaderPtr&, const proto::PartitionId&, KeyValueMap&));
    MOCK_METHOD2(loadGenerationMeta, bool(const config::ResourceReaderPtr, const std::string&));
    MOCK_METHOD1(createBuildFlow, workflow::BuildFlow*(const config::ResourceReaderPtr&));

private:
    workflow::BuildFlow* doCreateBuildFlow(const config::ResourceReaderPtr& resourceReader)
    {
        return ProcessorServiceImpl::createBuildFlow(resourceReader);
    }

    void updateResourceKeeperMap(bool& needRestart) { needRestart = true; }

    bool doStartProcessFlow(const config::ResourceReaderPtr& resourceReader, const proto::PartitionId& pid,
                            KeyValueMap& kvMap)
    {
        return ProcessorServiceImpl::startProcessFlow(resourceReader, pid, kvMap);
    }
};

typedef ::testing::StrictMock<MockProcessorServiceImpl> StrictMockProcessorServiceImpl;
typedef ::testing::NiceMock<MockProcessorServiceImpl> NiceMockProcessorServiceImpl;

}} // namespace build_service::worker
