#include "swift/log/BrokerLogClosure.h"

#include <google/protobuf/stubs/callback.h>
#include <iosfwd>

#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;

namespace swift {
namespace util {

class FakeClosure : public google::protobuf::Closure {
public:
    FakeClosure() { done = false; }
    void Run() { done = true; }

public:
    bool done;
};

class ConsumptionLogClosureTest : public TESTBASE {
public:
    void setUp(){};
    void tearDown(){};
};

class ProductionLogClosureTest : public TESTBASE {
public:
    void setUp(){};
    void tearDown(){};
};

class MessageIdLogClosureTest : public TESTBASE {
public:
    void setUp(){};
    void tearDown(){};
};

void makeConsumptionRequest(ConsumptionRequest &request) {
    request.set_topicname("test_topic");
    request.set_partitionid(10);
    request.set_startid(100);
    request.set_count(20);
    request.set_sessionid(153222);
    Filter filter;
    filter.set_from(0);
    filter.set_uint8filtermask(333);
    *request.mutable_filter() = filter;
    request.set_maxtotalsize(6710);
    request.add_requiredfieldnames("nid");
    request.add_requiredfieldnames("price");
    request.set_needcompress(true);
    request.set_fieldfilterdesc("nid IN 124");
    request.set_candecompressmsg(true);
    request.set_starttimestamp(153111);
    ClientVersionInfo *version = request.mutable_versioninfo();
    version->set_version("1.9.2");
    version->set_supportfb(true);
    request.set_sessionidextend(222);
    request.set_committedcheckpoint(33);
}

void makeMessageResponse(MessageResponse &response) {
    ErrorInfo *error = response.mutable_errorinfo();
    error->set_errcode(ERROR_CLIENT_INIT_TWICE);
    error->set_errmsg("incident error");
    protocol::Message *msg = response.add_msgs();
    ;
    msg->set_msgid(11);
    msg->set_data("xxxx");
    response.set_acceptedmsgcount(100);
    response.set_maxallowmsglen(4);
    response.set_maxmsgid(153222);
    response.set_maxtimestamp(153999);
    response.set_nextmsgid(153442);
    response.set_nexttimestamp(153444);
    response.set_compressedmsgs("aaa");
    response.set_acceptedmsgbeginid(10);
    response.set_committedid(11);
    response.set_sessionid(444);
    response.set_messageformat(MF_FB);
    response.set_fbmsgs("bbb");
    response.set_hasmergedmsg(true);
    response.set_totalmsgcount(200);
    response.set_topicversion(3333);
}

void makeProductionRequest(ProductionRequest &request) {
    request.set_topicname("test_topic");
    request.set_partitionid(10);
    protocol::Message *msg = request.add_msgs();
    ;
    msg->set_msgid(11);
    msg->set_data("xxxx");
    request.set_compressedmsgs("aaa");
    request.set_compressmsginbroker(true);
    request.set_sessionid(444);
    request.set_fbmsgs("bbb");
    request.set_messageformat(MF_FB);
    ClientVersionInfo *version = request.mutable_versioninfo();
    version->set_version("1.9.2");
    version->set_supportfb(true);
}

void makeMessageIdRequest(MessageIdRequest &request) {
    request.set_topicname("test_topic");
    request.set_partitionid(10);
    request.set_timestamp(153999);
    request.set_version("version_1.9.2");
    request.set_sessionid(444);
    ClientVersionInfo *version = request.mutable_versioninfo();
    version->set_version("1.9.2");
    version->set_supportfb(true);
}

void makeMessageIdResponse(MessageIdResponse &response) {
    ErrorInfo *error = response.mutable_errorinfo();
    error->set_errcode(ERROR_CLIENT_INIT_TWICE);
    error->set_errmsg("incident error");
    response.set_msgid(11);
    response.set_timestamp(153000);
    response.set_maxmsgid(153442);
    response.set_maxtimestamp(153999);
    response.set_version("version_1.9.2");
    response.set_sessionid(444);
    response.set_topicversion(3333);
}

TEST_F(ConsumptionLogClosureTest, testAccessLog) {
    FakeClosure closure;
    ConsumptionRequest request;
    MessageResponse response;
    { ConsumptionLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert empty log right
    makeConsumptionRequest(request);
    makeMessageResponse(response);
    { ConsumptionLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert log right
}

TEST_F(ProductionLogClosureTest, testAccessLog) {
    FakeClosure closure;
    ProductionRequest request;
    MessageResponse response;
    { ProductionLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert empty log right
    makeProductionRequest(request);
    makeMessageResponse(response);
    { ProductionLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert log right
}

TEST_F(ProductionLogClosureTest, testSetBeforeLogCallBack) {
    FakeClosure closure;
    ProductionRequest request;
    MessageResponse response;
    int cbCount = 0;
    int errorNoneCount = 0;
    auto callBack = [&cbCount, &errorNoneCount](MessageResponse *response) {
        cbCount++;
        const protocol::ErrorInfo &ecInfo = response->errorinfo();
        auto ec = ecInfo.errcode();
        if (ec == ERROR_NONE) {
            errorNoneCount++;
        }
    };

    {
        ProductionLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setBeforeLogCallBack(callBack);
    }
    ASSERT_EQ(1, cbCount);
    // assert empty log right
    makeProductionRequest(request);
    makeMessageResponse(response);
    {
        ProductionLogClosure logClosure(&request, &response, &closure, "test");
        logClosure.setBeforeLogCallBack(callBack);
    }
    ASSERT_EQ(2, cbCount);
    ASSERT_EQ(1, errorNoneCount);
    // assert log right
}

TEST_F(MessageIdLogClosureTest, testAccessLog) {
    FakeClosure closure;
    MessageIdRequest request;
    MessageIdResponse response;
    { MessageIdLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert empty log right
    makeMessageIdRequest(request);
    makeMessageIdResponse(response);
    { MessageIdLogClosure logClosure(&request, &response, &closure, "test"); }
    // assert log right
}

}; // namespace util
}; // namespace swift
