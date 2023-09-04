#include "swift/admin/WorkerErrorHandler.h"

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class WorkerErrorHandlerTest : public TESTBASE {
public:
    WorkerErrorHandlerTest();
    ~WorkerErrorHandlerTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, WorkerErrorHandlerTest);
using namespace swift::protocol;
using namespace std;

WorkerErrorHandlerTest::WorkerErrorHandlerTest() {}

WorkerErrorHandlerTest::~WorkerErrorHandlerTest() {}

void WorkerErrorHandlerTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void WorkerErrorHandlerTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(WorkerErrorHandlerTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    AUTIL_LOG(DEBUG, "Begin Test!");

    WorkerErrorHandler handler;
    handler.setErrorLimit(2);
    string address = "10.250.12.23:123";

    handler.addError(address, ROLE_TYPE_ADMIN, ERROR_BROKER_FS_OPERATE, 123, "567");

    ErrorRequest request;
    protocol::WorkerErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());
    ASSERT_EQ(address, response.errors(0).workeraddress());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(int64_t(123), response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

} // namespace admin
} // namespace swift
