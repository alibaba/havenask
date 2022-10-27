#ifndef ISEARCH_BS_MOCKBUILDTASKBASE_H
#define ISEARCH_BS_MOCKBUILDTASKBASE_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/BuildTask.h"
#include "build_service/workflow/test/MockBuildFlow.h"

namespace build_service {
namespace task_base {

class MockBuildTask : public BuildTask
{
public:
    MockBuildTask() {
        ON_CALL(*this, createBuildFlow()).WillByDefault(
                Invoke(&workflow::MockBuildFlow::createNice));
    }
    ~MockBuildTask() {}

private:
    MOCK_CONST_METHOD0(createBuildFlow, workflow::BuildFlow*());
};

typedef ::testing::StrictMock<MockBuildTask> StrictMockBuildTask;
typedef ::testing::NiceMock<MockBuildTask> NiceMockBuildTask;

}
}

#endif //ISEARCH_BS_MOCKBUILDTASKBASE_H
