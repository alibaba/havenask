#pragma once

#include "suez/deploy/IndexDeployer.h"
#include "suez/table/IIndexlibAdapter.h"
#include "suez/table/SuezIndexPartition.h"
#include "suez/table/SuezRawFilePartition.h"
#include "unittest/unittest.h"

namespace suez {

class MockIndexDeployer : public IndexDeployer {
public:
    MockIndexDeployer(const PartitionId &pid, DeployManager *deployManager) : IndexDeployer(pid, deployManager) {}
    ~MockIndexDeployer() {}

public:
    DeployStatus deploy(const IndexPathDetail &pathDetail,
                        IncVersion oldVersionId,
                        IncVersion newVersionId,
                        const indexlibv2::config::TabletOptions &baseTabletOptions,
                        const indexlibv2::config::TabletOptions &targetTabletOption) override {
        _oldVersionId = oldVersionId;
        _newVersionId = newVersionId;
        return _status;
    }
    void cancel() override {}
    bool cleanDoneFiles(const std::string &localRootPath, const std::set<IncVersion> &inUseVersions) override {
        return true;
    }

    void setStatus(DeployStatus s) { _status = s; }
    DeployStatus getStatus() const { return _status; }
    IncVersion getOldVersionId() const { return _oldVersionId; }
    IncVersion getNewVersionId() const { return _newVersionId; }

public:
    IncVersion _oldVersionId = INVALID_VERSION;
    IncVersion _newVersionId = INVALID_VERSION;
    DeployStatus _status = DS_UNKNOWN;
};

class MockSuezRawFilePartition : public SuezRawFilePartition {
public:
    MockSuezRawFilePartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta)
        : SuezRawFilePartition(tableResource, partitionMeta) {}

public:
    MOCK_CONST_METHOD0(isRecovered, bool());
    MOCK_METHOD2(doDeploy, DeployStatus(const std::string &, const std::string &));
    MOCK_METHOD1(doLoad, StatusAndError<TableStatus>(const std::string &));
};
typedef ::testing::NiceMock<MockSuezRawFilePartition> NiceMockSuezRawFilePartition;
typedef ::testing::StrictMock<MockSuezRawFilePartition> StrictMockSuezRawFilePartition;

class MockSuezIndexPartition : public SuezIndexPartition {
public:
    MockSuezIndexPartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta)
        : SuezIndexPartition(tableResource, partitionMeta) {
        _indexDeployer.reset(new MockIndexDeployer(tableResource.pid, tableResource.deployManager));
    }

public:
    MockIndexDeployer *getIndexDeployer() const { return dynamic_cast<MockIndexDeployer *>(_indexDeployer.get()); }

    MOCK_CONST_METHOD0(isRecovered, bool());
    MOCK_CONST_METHOD2(loadTableConfig, bool(const TargetPartitionMeta &, TableConfig &));
    MOCK_METHOD2(doDeployConfig, DeployStatus(const std::string &, const std::string &));
    MOCK_METHOD2(doDeployIndex, DeployStatus(const TargetPartitionMeta &, bool));
    MOCK_CONST_METHOD2(createIndexlibAdapter, std::unique_ptr<IIndexlibAdapter>(const PartitionProperties &, uint64_t));
    MOCK_CONST_METHOD2(createTableBuilder,
                       TableBuilder *(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties));
    MOCK_CONST_METHOD1(createTableWriter, std::shared_ptr<TableWriter>(const PartitionProperties &));
    MOCK_METHOD2(initLogReplicator, bool(IIndexlibAdapter *, const PartitionProperties &));
    MOCK_METHOD0(closeLogReplicator, void());
    MOCK_METHOD1(cleanIndexFiles, void(const std::set<IncVersion> &inuseIncVersion));
};
typedef ::testing::NiceMock<MockSuezIndexPartition> NiceMockSuezIndexPartition;
typedef ::testing::StrictMock<MockSuezIndexPartition> StrictMockSuezIndexPartition;

class MockSuezPartition : public SuezPartition {
public:
    MockSuezPartition(const CurrentPartitionMetaPtr &partitionMeta,
                      const PartitionId &pid,
                      const kmonitor::MetricsReporterPtr &metricsReporter)
        : SuezPartition(partitionMeta, pid, metricsReporter) {}

public:
    MOCK_METHOD2(deploy, StatusAndError<DeployStatus>(const TargetPartitionMeta &, bool));
    MOCK_CONST_METHOD0(isRecovered, bool());
    MOCK_METHOD0(cancelDeploy, void());
    MOCK_METHOD0(cancelLoad, void());
    MOCK_METHOD2(load, StatusAndError<TableStatus>(const TargetPartitionMeta &, bool));
    MOCK_METHOD0(unload, void());
    MOCK_METHOD0(stopRt, void());
    MOCK_METHOD0(suspendRt, void());
    MOCK_METHOD0(resumeRt, void());
    MOCK_CONST_METHOD0(hasRt, bool());
    MOCK_CONST_METHOD0(isInUse, bool());
    MOCK_CONST_METHOD0(getUseCount, int32_t());
    MOCK_CONST_METHOD0(getPartitionData, SuezPartitionDataPtr());
    MOCK_CONST_METHOD0(needCommit, bool());
    MOCK_METHOD0(commit, std::pair<bool, TableVersion>());
    MOCK_METHOD1(stopWrite, void(SuezPartition::StopWriteOption));
    MOCK_METHOD0(stopLogReplicator, void());
    MOCK_CONST_METHOD0(getTableWriter, std::shared_ptr<TableWriter>());
    MOCK_METHOD1(setTableWriter, void(const std::shared_ptr<TableWriter> &));
    MOCK_METHOD1(cleanIndexFiles, void(const std::set<IncVersion> &inuseIncVersion));
};
typedef ::testing::NiceMock<MockSuezPartition> NiceMockSuezPartition;
typedef ::testing::StrictMock<MockSuezPartition> StrictMockSuezPartition;

} // namespace suez
