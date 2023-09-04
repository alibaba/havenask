#include "swift/admin/PartitionErrorHandler.h"

#include <cstddef>
#include <deque>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
namespace swift {
namespace admin {

class PartitionErrorHandlerTest : public TESTBASE {
public:
    PartitionErrorHandlerTest();
    ~PartitionErrorHandlerTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, PartitionErrorHandlerTest);
using namespace swift::protocol;

PartitionErrorHandlerTest::PartitionErrorHandlerTest() {}

PartitionErrorHandlerTest::~PartitionErrorHandlerTest() {}

void PartitionErrorHandlerTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void PartitionErrorHandlerTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(PartitionErrorHandlerTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(2);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 123, "567");
    ErrorRequest request;
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());
    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(int64_t(123), response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

TEST_F(PartitionErrorHandlerTest, testAddRepeatedError) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(2);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 123, "567");
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 567, "567");
    ASSERT_EQ(size_t(1), handler._errQueue.size());
    ErrorRequest request;
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());
    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(int64_t(123), response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

TEST_F(PartitionErrorHandlerTest, testAddMoreError) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(2);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 123, "567");
    partitionId1.set_id(1);
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 567, "567");
    ASSERT_EQ(size_t(2), handler._errQueue.size());
    partitionId1.set_id(2);
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, 567, "567");
    ASSERT_EQ(size_t(2), handler._errQueue.size());

    ErrorRequest request;
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(2, response.errors_size());

    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(int64_t(567), response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

TEST_F(PartitionErrorHandlerTest, testChoiceByTime) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(3);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    int64_t time = TimeUtility::currentTime();
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time - 30 * 1000 * 1000, "567");
    partitionId1.set_id(1);
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time, "567");
    handler.addError(partitionId1, ERROR_NONE, time, "567");

    ASSERT_EQ(size_t(3), handler._errQueue.size());

    ErrorRequest request;
    request.set_time(1);
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());

    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(time, response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

TEST_F(PartitionErrorHandlerTest, testChoiceByCount) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(3);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    int64_t time = TimeUtility::currentTime();
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time - 30 * 1000 * 1000, "567");
    partitionId1.set_id(1);
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time, "567");

    handler.addError(partitionId1, ERROR_NONE, time, "567");

    ASSERT_EQ(size_t(3), handler._errQueue.size());

    ErrorRequest request;
    request.set_count(1);
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());

    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(time, response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}

TEST_F(PartitionErrorHandlerTest, testGetPartitionError) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    PartitionErrorHandler handler;
    handler.setErrorLimit(3);
    PartitionId partitionId1;
    partitionId1.set_topicname("news");
    partitionId1.set_id(0);

    int64_t time = TimeUtility::currentTime();
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time - 30 * 1000 * 1000, "567");
    partitionId1.set_id(1);
    handler.addError(partitionId1, ERROR_BROKER_FS_OPERATE, time, "567");
    handler.addError(partitionId1, ERROR_NONE, time, "567");

    ASSERT_EQ(size_t(3), handler._errQueue.size());

    ErrorRequest request;
    request.set_count(3);
    request.set_time(1);
    protocol::PartitionErrorResponse response;
    handler.getError(request, response);
    ASSERT_EQ(1, response.errors_size());

    ASSERT_TRUE(partitionId1 == response.errors(0).partitionid());
    ASSERT_EQ(ERROR_BROKER_FS_OPERATE, response.errors(0).errcode());
    ASSERT_EQ(time, response.errors(0).errtime());
    ASSERT_EQ(string("567"), response.errors(0).errmsg());
}
} // namespace admin
} // namespace swift
