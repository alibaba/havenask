#include "swift/admin/ErrorHandler.h"

#include <cstddef>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/admin/PartitionErrorHandler.h"
#include "swift/admin/test/MessageConstructor.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "unittest/unittest.h"

namespace swift {
namespace admin {

class ErrorHandlerTest : public TESTBASE {
public:
    ErrorHandlerTest();
    ~ErrorHandlerTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, ErrorHandlerTest);
using namespace swift::protocol;
using namespace autil;
using namespace std;

ErrorHandlerTest::ErrorHandlerTest() {}

ErrorHandlerTest::~ErrorHandlerTest() {}

void ErrorHandlerTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void ErrorHandlerTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(ErrorHandlerTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    HeartbeatInfo heartbeat = MessageConstructor::ConstructHeartbeatInfo(
        "10.250.12.23:1023", ROLE_TYPE_BROKER, true, "topicName", 1, PARTITION_STATUS_RUNNING);
    int64_t time1 = TimeUtility::currentTime();
    heartbeat.set_errcode(ERROR_BROKER_READ_FILE);
    heartbeat.set_errtime(time1);

    int64_t time2 = TimeUtility::currentTime();
    heartbeat.mutable_tasks(0)->set_errcode(ERROR_BROKER_FS_OPERATE);
    heartbeat.mutable_tasks(0)->set_errtime(time2);

    ErrorHandler handler;
    handler.extractError(heartbeat);

    ErrorRequest request;
    PartitionErrorResponse response;
    handler.getPartitionError(request, response);
    ASSERT_EQ(1, response.errors_size());
    ASSERT_TRUE(heartbeat.tasks(0).taskinfo().partitionid() == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(time2, response.errors(0).errtime());
    ASSERT_EQ(string(""), response.errors(0).errmsg());

    WorkerErrorResponse workerResponse;
    handler.getWorkerError(request, workerResponse);
    ASSERT_EQ(1, workerResponse.errors_size());
    ASSERT_EQ(ROLE_TYPE_BROKER, workerResponse.errors(0).role());
    ASSERT_EQ(string("10.250.12.23:1023"), workerResponse.errors(0).workeraddress());
    ASSERT_EQ(ERROR_BROKER_READ_FILE, workerResponse.errors(0).errcode());
    ASSERT_EQ(time1, workerResponse.errors(0).errtime());
    ASSERT_EQ(string(""), workerResponse.errors(0).errmsg());

    // test partition error is ERROR_NONE
    ASSERT_EQ((size_t)1, handler._partitionHandler.getErrorQueueSize());
    PartitionErrorResponse response2;
    heartbeat.mutable_tasks(0)->set_errcode(ERROR_NONE);
    handler.extractError(heartbeat);
    ASSERT_EQ((size_t)1, handler._partitionHandler.getErrorQueueSize());
}

TEST_F(ErrorHandlerTest, testSetErrorMsg) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    HeartbeatInfo heartbeat = MessageConstructor::ConstructHeartbeatInfo(
        "10.250.12.23:1023", ROLE_TYPE_BROKER, true, "topicName", 1, PARTITION_STATUS_RUNNING);
    int64_t time1 = TimeUtility::currentTime();
    heartbeat.set_errcode(ERROR_BROKER_READ_FILE);
    heartbeat.set_errtime(time1);
    heartbeat.set_errmsg("error msg1");

    int64_t time2 = TimeUtility::currentTime();
    heartbeat.mutable_tasks(0)->set_errcode(ERROR_BROKER_FS_OPERATE);
    heartbeat.mutable_tasks(0)->set_errtime(time2);
    heartbeat.mutable_tasks(0)->set_errmsg("error msg2");

    ErrorHandler handler;
    handler.extractError(heartbeat);

    ErrorRequest request;
    PartitionErrorResponse response;
    handler.getPartitionError(request, response);
    ASSERT_EQ(1, response.errors_size());
    ASSERT_TRUE(heartbeat.tasks(0).taskinfo().partitionid() == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(time2, response.errors(0).errtime());
    ASSERT_EQ(string("error msg2"), response.errors(0).errmsg());

    WorkerErrorResponse workerResponse;
    handler.getWorkerError(request, workerResponse);
    ASSERT_EQ(1, workerResponse.errors_size());
    ASSERT_EQ(ROLE_TYPE_BROKER, workerResponse.errors(0).role());
    ASSERT_EQ(string("10.250.12.23:1023"), workerResponse.errors(0).workeraddress());
    ASSERT_EQ(ERROR_BROKER_READ_FILE, workerResponse.errors(0).errcode());
    ASSERT_EQ(time1, workerResponse.errors(0).errtime());
    ASSERT_EQ(string("error msg1"), workerResponse.errors(0).errmsg());
}

TEST_F(ErrorHandlerTest, testDeadWorker) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    HeartbeatInfo heartbeat = MessageConstructor::ConstructHeartbeatInfo(
        "10.250.12.23:1023", ROLE_TYPE_BROKER, false, "topicName", 1, PARTITION_STATUS_RUNNING);
    int64_t time1 = TimeUtility::currentTime();
    heartbeat.set_errcode(ERROR_BROKER_READ_FILE);
    heartbeat.set_errtime(time1);
    heartbeat.set_errmsg("error msg1");

    ErrorHandler handler;
    handler.extractError(heartbeat);

    ErrorRequest request;
    WorkerErrorResponse workerResponse;
    handler.getWorkerError(request, workerResponse);
    ASSERT_EQ(1, workerResponse.errors_size());
    ASSERT_EQ(ROLE_TYPE_BROKER, workerResponse.errors(0).role());
    ASSERT_EQ(string("10.250.12.23:1023"), workerResponse.errors(0).workeraddress());
    ASSERT_EQ(ERROR_WORKER_DEAD, workerResponse.errors(0).errcode());
    ASSERT_TRUE(time1 < workerResponse.errors(0).errtime());
    ASSERT_EQ(ErrorCode_Name(ERROR_WORKER_DEAD), workerResponse.errors(0).errmsg());
}

TEST_F(ErrorHandlerTest, testClear) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    HeartbeatInfo heartbeat = MessageConstructor::ConstructHeartbeatInfo(
        "10.250.12.23:1023", ROLE_TYPE_BROKER, true, "topicName", 1, PARTITION_STATUS_RUNNING);
    int64_t time1 = TimeUtility::currentTime();
    heartbeat.set_errcode(ERROR_BROKER_READ_FILE);
    heartbeat.set_errtime(time1);
    heartbeat.set_errmsg("error msg1");

    int64_t time2 = TimeUtility::currentTime();
    heartbeat.mutable_tasks(0)->set_errcode(ERROR_BROKER_FS_OPERATE);
    heartbeat.mutable_tasks(0)->set_errtime(time2);
    heartbeat.mutable_tasks(0)->set_errmsg("error msg2");

    ErrorHandler handler;
    handler.extractError(heartbeat);

    ErrorRequest request;
    PartitionErrorResponse response;
    handler.getPartitionError(request, response);
    ASSERT_EQ(1, response.errors_size());
    WorkerErrorResponse workerResponse;
    handler.getWorkerError(request, workerResponse);
    ASSERT_EQ(1, workerResponse.errors_size());

    response.clear_errors();
    workerResponse.clear_errors();
    handler.clear();
    handler.getPartitionError(request, response);
    ASSERT_EQ(0, response.errors_size());
    handler.getWorkerError(request, workerResponse);
    ASSERT_EQ(0, workerResponse.errors_size());
}

} // namespace admin
} // namespace swift
