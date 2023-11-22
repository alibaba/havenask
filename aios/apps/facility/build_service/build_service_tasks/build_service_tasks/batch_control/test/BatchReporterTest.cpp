#include "build_service_tasks/batch_control/BatchReporter.h"

#include <cstdint>
#include <iostream>
#include <map>
#include <stddef.h>
#include <string>
#include <vector>

#include "aios/network/anet/httppacket.h"
#include "aios/network/anet/ipacketfactory.h"
#include "aios/network/anet/ipackethandler.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service_tasks/batch_control/BatchControlWorker.h"
#include "build_service_tasks/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace anet;

using namespace testing;
using namespace build_service::proto;

namespace build_service { namespace task_base {

typedef BatchControlWorker::BatchOp BatchOp;
typedef BatchControlWorker::BatchInfo BatchInfo;
typedef BatchReporter::PacketHandler PacketHandler;
typedef BatchReporter::RequestBody RequestBody;

class BatchReporterTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void BatchReporterTest::setUp() {}

void BatchReporterTest::tearDown() {}

TEST_F(BatchReporterTest, testInit)
{
    BatchReporter reporter;
    proto::BuildId buildId;
    vector<string> clusters;
    string host = "127.0.0.1";
    int32_t port = 100;
    // empty cluster
    ASSERT_FALSE(reporter.init(buildId, clusters));
    clusters.push_back("c1");
    clusters.push_back("c2");
    ASSERT_TRUE(reporter.init(buildId, clusters));

    // error host
    ASSERT_FALSE(reporter.resetHost("", port));
    ASSERT_TRUE(reporter.resetHost(host, port));
    ASSERT_EQ("[\n  \"c1\",\n  \"c2\"\n]", reporter._clusterNames);
    ASSERT_EQ("{\n\"c1\":\n  \"0\",\n\"c2\":\n  \"0\"\n}", reporter._schemaIdMap);
    ASSERT_EQ(host, reporter._host);
    ASSERT_EQ("tcp:127.0.0.1:100", reporter._endpoint);
}

TEST_F(BatchReporterTest, testHandlePacket)
{
    BatchInfo batch;
    {
        // invalid status code
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(1);
        PacketHandler handler(batch);
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_TRUE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
    {
        // error response, invalid json string
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(200);
        PacketHandler handler(batch);
        string response = "{\"status\":123";
        packet->setBody(response.c_str(), response.size());
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_TRUE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
    {
        // error response
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(200);
        PacketHandler handler(batch);
        string response = "{\"errorMessage\":[\"error\"]}";
        packet->setBody(response.c_str(), response.size());
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_TRUE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
    {
        // error response
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(200);
        PacketHandler handler(batch);
        string response = "{\"errorMessage\":[]}";
        packet->setBody(response.c_str(), response.size());
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_TRUE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
    {
        // success
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(200);
        PacketHandler handler(batch);
        string response = "{\"status\":1}";
        packet->setBody(response.c_str(), response.size());
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_FALSE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
    {
        // success
        HTTPPacket* packet = new HTTPPacket();
        packet->setStatusCode(200);
        PacketHandler handler(batch);
        string response = "";
        packet->setBody(response.c_str(), response.size());
        ASSERT_EQ(IPacketHandler::FREE_CHANNEL, handler.handlePacket(packet, NULL));
        ASSERT_FALSE(handler.hasError());
        ASSERT_TRUE(handler.isDone());
    }
}

TEST_F(BatchReporterTest, testJsonize)
{
    proto::BuildId buildId;
    buildId.set_appname("test");
    buildId.set_generationid(12345);
    RequestBody body(buildId);
    body._graphFileName = "abc.json";
    body._graphId = "graph1";
    body._params["a"] = "a";
    body._params["b"] = "b";

    string jsonStr = ToJsonString(body);
    cout << jsonStr << endl;
    RequestBody body2(buildId);
    FromJsonString(body2, jsonStr);

    ASSERT_EQ("test", body2._buildId._buildId.appname());
    ASSERT_EQ(12345, body2._buildId._buildId.generationid());
    ASSERT_EQ(body._graphFileName, body2._graphFileName);
    ASSERT_EQ(body._graphId, body2._graphId);
    ASSERT_EQ(body._params["a"], body2._params["a"]);
    ASSERT_EQ(body._params["b"], body2._params["b"]);
}

TEST_F(BatchReporterTest, testPrepareParams)
{
    proto::BuildId buildId;
    buildId.set_appname("test");
    buildId.set_generationid(12345);

    BatchReporter reporter;
    proto::DataDescription ds;
    ds["a"] = "a";
    ds["b"] = "b";
    reporter.setDataDescription(ds);
    BatchReporter::RequestBody body(buildId);
    vector<int64_t> skipBatchIds = {3, 7, 9};
    vector<int64_t> combinedIds = {8};
    reporter.prepareParams(BatchInfo(BatchOp::begin, 123, -1, 1), 10, 8, skipBatchIds, combinedIds, body);
    ASSERT_EQ("1", body._params["batchMask"]);
    ASSERT_EQ("123", body._params["beginTime"]);
    ASSERT_EQ("[\n  {\n  \"a\":\n    \"a\",\n  \"b\":\n    \"b\"\n  }\n]", body._params["dataDescriptions"]);
    ASSERT_EQ("0", body._params["startDataDescriptionId"]);
    ASSERT_EQ("10", body._params["batchId"]);
    ASSERT_EQ("8", body._params["lastBatchId"]);
    ASSERT_EQ("[\n  \"3\",\n  \"7\",\n  \"9\"\n]", body._params["skipMergeBatchIds"]);
    ASSERT_EQ("[\n  \"8\"\n]", body._params["combinedBatchIds"]);
}

void innerTestPrepareSkipMergeBatchIds(int64_t batchId, int64_t commitVersionId, int64_t skipCount,
                                       int64_t expectNewVersionId, const vector<int64_t>& expectSkipMergeIds,
                                       const vector<int64_t>& expectCombinedIds)
{
    int64_t newVersionId = 0;
    vector<int64_t> skipMergeIds;
    vector<int64_t> combinedIds;
    BatchReporter::prepareSkipMergeBatchIds(batchId, commitVersionId, skipCount, &newVersionId, &skipMergeIds,
                                            &combinedIds);
    ASSERT_EQ(expectNewVersionId, newVersionId);
    ASSERT_EQ(expectSkipMergeIds, skipMergeIds);
    ASSERT_EQ(expectCombinedIds, combinedIds);
}

TEST_F(BatchReporterTest, testPrepareSkipMergeBatchIds)
{
    innerTestPrepareSkipMergeBatchIds(2, -1, 3, -1, {}, {});
    innerTestPrepareSkipMergeBatchIds(3, -1, 3, -1, {}, {});
    innerTestPrepareSkipMergeBatchIds(4, -1, 3, 1, {}, {1});
    innerTestPrepareSkipMergeBatchIds(5, -1, 3, 2, {1}, {2});
    innerTestPrepareSkipMergeBatchIds(6, -1, 3, 3, {1, 2}, {3});
    innerTestPrepareSkipMergeBatchIds(7, -1, 3, 4, {2, 3}, {1, 4});
    innerTestPrepareSkipMergeBatchIds(8, -1, 3, 5, {1, 3, 4}, {2, 5});
    innerTestPrepareSkipMergeBatchIds(8, 4, 3, 4, {}, {});
    innerTestPrepareSkipMergeBatchIds(8, 1, 3, 4, {2, 3}, {4});
    innerTestPrepareSkipMergeBatchIds(8, 7, 3, 7, {}, {});
    innerTestPrepareSkipMergeBatchIds(12, 7, 3, 7, {}, {});
    innerTestPrepareSkipMergeBatchIds(13, 7, 3, 10, {8, 9}, {10});
    innerTestPrepareSkipMergeBatchIds(14, 10, 3, 10, {}, {});
    innerTestPrepareSkipMergeBatchIds(15, 10, 3, 10, {}, {});
    innerTestPrepareSkipMergeBatchIds(16, 10, 3, 13, {11, 12}, {13});
}

}} // namespace build_service::task_base
