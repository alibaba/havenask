#include "swift/broker/replication/MessageConsumer.h"
#include "swift/broker/replication/MessageProducer.h"
#include "unittest/unittest.h"

namespace swift {
namespace replication {

class MockMessageProducer : public MessageProducer {
public:
    MockMessageProducer(int64_t globalId, bool readCommittedMsg) : MessageProducer(globalId, readCommittedMsg) {}

public:
    MOCK_METHOD2(produce, void(protocol::ConsumptionRequest *, protocol::MessageResponse *));
};
using NiceMockMessageProducer = testing::NiceMock<MockMessageProducer>;

class MockMessageConsumer : public MessageConsumer {
public:
    MOCK_METHOD1(doConsume, autil::Result<int64_t>(protocol::MessageResponse *));
};
using NiceMockMessageConsumer = testing::NiceMock<MockMessageConsumer>;

} // namespace replication
} // namespace swift
