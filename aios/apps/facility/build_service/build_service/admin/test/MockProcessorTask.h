#pragma once

#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "unittest/unittest.h"

namespace build_service { namespace admin {

class MockProcessorTask : public ProcessorTask
{
public:
    MockProcessorTask(const proto::BuildId& buildId, proto::BuildStep step, const TaskResourceManagerPtr& resMgr)
        : ProcessorTask(buildId, step, resMgr)
    {
    }
    ~MockProcessorTask() {}

private:
    MockProcessorTask(const MockProcessorTask&);
    MockProcessorTask& operator=(const MockProcessorTask&);

public:
    typedef std::vector<std::pair<size_t, uint32_t>> UpdatedMinorVersions;
    MOCK_METHOD(int64_t, getCurrentTime, (), (const));
    MOCK_METHOD(bool, updateSwiftWriterVersion, (const proto::ProcessorNodes&, uint32_t, const UpdatedMinorVersions&),
                (const));
    MOCK_METHOD(common::SwiftAdminFacadePtr, getSwiftAdminFacade, (const std::string& clusterName), (const override));

private:
    BS_LOG_DECLARE();
};

typedef ::testing::NiceMock<MockProcessorTask> NiceMockProcessorTask;

BS_TYPEDEF_PTR(MockProcessorTask);

}} // namespace build_service::admin
