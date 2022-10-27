#ifndef ISEARCH_BS_MOCKBROKERFACTORY_H
#define ISEARCH_BS_MOCKBROKERFACTORY_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/workflow/test/MockProducer.h"
#include "build_service/workflow/test/MockConsumer.h"

namespace build_service {
namespace workflow {

class MockBrokerFactory : public BrokerFactory
{
public:
    MockBrokerFactory() {
        ON_CALL(*this, createRawDocProducer(_)).WillByDefault(
                Invoke(std::bind(&MockRawDocProducer::createNice)));
        ON_CALL(*this, createRawDocConsumer(_)).WillByDefault(
                Invoke(std::bind(&MockRawDocConsumer::createNice)));

        ON_CALL(*this, createProcessedDocProducer(_)).WillByDefault(
                Invoke(std::bind(&MockProcessedDocProducer::createNice)));
        ON_CALL(*this, createProcessedDocConsumer(_)).WillByDefault(
                Invoke(std::bind(&MockProcessedDocConsumer::createNice)));
    }
    ~MockBrokerFactory() {}

public:
    MOCK_METHOD1(createRawDocProducer, RawDocProducer*(const RoleInitParam&));
    MOCK_METHOD1(createRawDocConsumer, RawDocConsumer*(const RoleInitParam&));
    MOCK_METHOD1(createProcessedDocProducer, ProcessedDocProducer*(const RoleInitParam&));
    MOCK_METHOD1(createProcessedDocConsumer, ProcessedDocConsumer*(const RoleInitParam&));
    MOCK_METHOD1(initCounterMap, bool(RoleInitParam &initParam));
    
private:
    MOCK_METHOD0(stopProduceProcessedDoc, void());
};

typedef ::testing::StrictMock<MockBrokerFactory> StrictMockBrokerFactory;
typedef ::testing::NiceMock<MockBrokerFactory> NiceMockBrokerFactory;

}
}

#endif //ISEARCH_BS_MOCKBROKERFACTORY_H
