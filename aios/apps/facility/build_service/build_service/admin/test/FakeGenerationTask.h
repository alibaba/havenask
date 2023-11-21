#pragma once

#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/test/FakeBuildServiceTaskFactory.h"
#include "build_service/admin/test/FakeMultiClusterRealtimeSchemaListKeeper.h"
#include "build_service/common/test/FakeBrokerTopicAccessor.h"
#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FakeGenerationTask : public GenerationTask
{
public:
    FakeGenerationTask(const std::string& indexPath, const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper)
        : GenerationTask(buildId, zkWrapper)
        , _indexPath(indexPath)
    {
        //_buildTask.reset(new FakeBuildTask(buildId, zkWrapper));
        _taskFactory.reset(new FakeBuildServiceTaskFactory());
        _taskFlowManager->TEST_setTaskFactory(_taskFactory);
        common::BrokerTopicAccessorPtr brokerTopicAccessor(new common::FakeBrokerTopicAccessor(_buildId, indexPath));
        _resourceManager->addResourceIgnoreExist(brokerTopicAccessor);
    }

    ~FakeGenerationTask() {}

private:
    FakeGenerationTask& operator=(const FakeGenerationTask&);

public:
    bool readIndexRoot(const std::string& configPath, std::string& indexRoot, std::string& fullBuilderTmpRoot) override
    {
        indexRoot = fullBuilderTmpRoot = _indexPath;
        return true;
    }
    std::shared_ptr<config::MultiClusterRealtimeSchemaListKeeper> getSchemaListKeeper() const override
    {
        if (_schemaListKeeper) {
            return _schemaListKeeper;
        }
        std::vector<std::string> clusterNames;
        if (!getAllClusterNames(clusterNames)) {
            return nullptr;
        }
        _schemaListKeeper.reset(new FakeMultiClusterRealtimeSchemaListKeeper);
        _schemaListKeeper->init(_indexRoot, clusterNames, _buildId.generationid());
        return _schemaListKeeper;
    }

private:
    std::string _indexPath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeGenerationTask);

}} // namespace build_service::admin
