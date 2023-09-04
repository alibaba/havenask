#ifndef ISEARCH_BS_MOCKWORKERNODE_H
#define ISEARCH_BS_MOCKWORKERNODE_H

#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MockWorkerNode : public proto::WorkerNodeBase
{
public:
    MockWorkerNode(const proto::PartitionId& partitionId) : proto::WorkerNodeBase(partitionId)
    {
        ON_CALL(*this, setTargetStatusStr(_))
            .WillByDefault(Invoke(std::bind(&MockWorkerNode::setTarget, this, std::placeholders::_1)));

        ON_CALL(*this, getTargetStatusStr(_, _))
            .WillByDefault(
                Invoke(std::bind(&MockWorkerNode::getTarget, this, std::placeholders::_1, std::placeholders::_2)));
        ON_CALL(*this, setCurrentStatusStr(_, _))
            .WillByDefault(Invoke(std::bind(&MockWorkerNode::setCurrent, this, std::placeholders::_1)));
        ON_CALL(*this, getCurrentStatusStr()).WillByDefault(Invoke(std::bind(&MockWorkerNode::getCurrent, this)));
    }
    ~MockWorkerNode() {}

public:
    MOCK_METHOD1(setTargetStatusStr, void(const std::string&));
    MOCK_CONST_METHOD2(getTargetStatusStr, void(std::string&, std::string&));
    MOCK_CONST_METHOD0(getRoleType, proto::RoleType());
    MOCK_METHOD2(setCurrentStatusStr, void(const std::string&, const std::string&));
    MOCK_CONST_METHOD0(getCurrentStatusStr, std::string());
    // MOCK_CONST_METHOD0(getWorkerProtocolVersion, int32_t);
    MOCK_METHOD1(setSlotInfo, void(const PBSlotInfos&));
    MOCK_METHOD0(changeSlots, void());
    MOCK_METHOD1(getToReleaseSlots, bool(PBSlotInfos&));
    MOCK_CONST_METHOD0(getCurrentIdentifier, std::string());
    MOCK_CONST_METHOD0(getTargetStatusJsonStr, std::string());
    MOCK_CONST_METHOD0(getCurrentStatusJsonStr, std::string());

    int32_t getWorkerProtocolVersion() const override { return UNKNOWN_WORKER_PROTOCOL_VERSION; }
    std::string getWorkerConfigPath() const override { return ""; }
    std::string getTargetConfigPath() const override { return ""; }
    bool isTargetSuspending() const override { return false; }

private:
    void setTarget(const std::string& t) { target = t; }
    void setCurrent(const std::string& c) { current = c; }
    const std::string& getCurrent() const { return current; }
    void getTarget(std::string& t, std::string& h) { t = target; }

private:
    std::string current;
    std::string target;
};

typedef ::testing::StrictMock<MockWorkerNode> StrictMockWorkerNode;
typedef ::testing::NiceMock<MockWorkerNode> NiceMockWorkerNode;

}} // namespace build_service::admin

#endif // ISEARCH_BS_MOCKWORKERNODE_H
