#pragma once

#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class MockSwiftAdminFacade : public SwiftAdminFacade
{
public:
    MockSwiftAdminFacade()
    {
        ON_CALL(*this, updateTopicVersionControl(_, _, _))
            .WillByDefault(Invoke(std::bind(&MockSwiftAdminFacade::doUpdateTopicVersionControl, this,
                                            std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)));
    }
    MOCK_METHOD(swift::network::SwiftAdminAdapterPtr, createSwiftAdminAdapter, (const std::string&), (override));
    MOCK_METHOD(bool, deleteTopic, (const std::string&, const std::string&), (override));
    MOCK_METHOD(bool, updateTopicVersionControl, (const std::string&, const std::string&, bool), (override));

    std::string getSwiftRoot(const std::string& topicConfigName) override
    {
        lastSwiftRoot = SwiftAdminFacade::getSwiftRoot(topicConfigName);
        return lastSwiftRoot;
    }
    std::string lastSwiftRoot;

private:
    MOCK_METHOD(bool, createTopic,
                (const swift::network::SwiftAdminAdapterPtr&, const std::string&, const config::SwiftTopicConfig&,
                 bool),
                (override));
    bool doUpdateTopicVersionControl(const std::string& topicConfigName, const std::string& topicName,
                                     bool needVersionControl)
    {
        return SwiftAdminFacade::updateTopicVersionControl(topicConfigName, topicName, needVersionControl);
    }
};

typedef ::testing::StrictMock<MockSwiftAdminFacade> StrictMockSwiftAdminFacade;
typedef ::testing::NiceMock<MockSwiftAdminFacade> NiceMockSwiftAdminFacade;

}} // namespace build_service::common
