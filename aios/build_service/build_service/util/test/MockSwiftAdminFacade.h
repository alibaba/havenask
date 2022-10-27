#ifndef ISEARCH_BS_MOCKSWIFTADMINFACADE_H
#define ISEARCH_BS_MOCKSWIFTADMINFACADE_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftAdminFacade.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service {
namespace util {

class MockSwiftAdminFacade : public util::SwiftAdminFacade
{
public:
    MockSwiftAdminFacade() {
        /* ON_CALL(*this, prepareBrokerTopic(_)).WillByDefault( */
        /*     Invoke(std::tr1::bind(&MockSwiftAdminFacade::doPrepareBrokerTopic, this, */
        /*                           std::tr1::placeholders::_1))); */

        /* ON_CALL(*this, destroyBrokerTopic(_)).WillByDefault( */
        /*     Invoke(std::tr1::bind(&MockSwiftAdminFacade::doDestroyBrokerTopic, this, */
        /*                           std::tr1::placeholders::_1))); */
    }
    MOCK_METHOD1(createSwiftAdminAdapter, SWIFT_NS(client)::SwiftAdminAdapterPtr (const std::string &));
    /* MOCK_METHOD1(prepareBrokerTopic, */
    /*                    bool(bool isFullBuildTopic)); */
    /* MOCK_METHOD1(destroyBrokerTopic, */
    /*                    bool(bool isFullBuildTopic)); */

    MOCK_METHOD2(deleteTopic,bool(const std::string &clusterName,
                                  bool isFullBuildTopic));
    MOCK_METHOD2(createTopic,bool(const std::string &clusterName,
                                  bool isFullBuildTopic));

    /* bool doPrepareBrokerTopic(bool isFullBuildTopic) */
    /* { */
    /*     return SwiftAdminFacade::prepareBrokerTopic(isFullBuildTopic); */
    /* } */

    /* bool doDestroyBrokerTopic(bool isFullBuildTopic) */
    /* { */
    /*     return SwiftAdminFacade::destroyBrokerTopic(isFullBuildTopic); */
    /* } */
    
};

typedef ::testing::StrictMock<MockSwiftAdminFacade> StrictMockSwiftAdminFacade;
typedef ::testing::NiceMock<MockSwiftAdminFacade> NiceMockSwiftAdminFacade;

}
}

#endif //ISEARCH_BS_MOCKSWIFTADMINFACADE_H
