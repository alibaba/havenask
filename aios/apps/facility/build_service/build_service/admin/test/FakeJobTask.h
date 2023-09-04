#ifndef ISEARCH_BS_FAKEJOBTASK_H
#define ISEARCH_BS_FAKEJOBTASK_H

#include "build_service/admin/JobTask.h"
#include "build_service/admin/test/FakeBuildServiceTaskFactory.h"
#include "build_service/common/test/FakeBrokerTopicAccessor.h"
#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FakeJobTask : public JobTask
{
public:
    FakeJobTask(const std::string& indexPath, const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
        : JobTask(buildId, zkWrapper)
        , _indexPath(indexPath)
    {
        _taskFactory.reset(new FakeBuildServiceTaskFactory());
        _taskFlowManager->TEST_setTaskFactory(_taskFactory);
        common::BrokerTopicAccessorPtr brokerTopicAccessor(new common::FakeBrokerTopicAccessor(_buildId, indexPath));
        _resourceManager->addResourceIgnoreExist(brokerTopicAccessor);
    }

    ~FakeJobTask() {}

private:
    FakeJobTask& operator=(const FakeJobTask&);

public:
    bool readIndexRoot(const std::string& configPath, std::string& indexRoot, std::string& fullBuilderTmpRoot) override
    {
        indexRoot = fullBuilderTmpRoot = _indexPath;
        return true;
    }

private:
    std::string _indexPath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeJobTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_FAKEGENERATIONTASK_H
