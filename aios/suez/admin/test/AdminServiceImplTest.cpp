#include "suez/admin/AdminServiceImpl.h"

#include <atomic>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iostream>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <thread>

#include "MockRoleManagerWrapper.h"
#include "TargetLoader.h"
#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "carbon/RoleManagerWrapper.h"
#include "carbon/legacy/GroupStatusWrapper.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "carbon/proto/Carbon.pb.h"
#include "compatible/RoleManagerWrapperImpl.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "hippo/proto/ErrorCode.pb.h"
#include "suez/admin/Admin.pb.h"
#include "suez/common/TimeRecorder.h"
#include "suez/sdk/JsonNodeRef.h"
#include "unittest/unittest.h"
#include "worker_framework/WorkerBase.h"
#include "worker_framework/ZkState.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace fslib;
using namespace fslib::fs;
using namespace carbon;
using namespace carbon::proto;
using namespace http_arpc;
using namespace hippo;
using namespace cm_basic;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, GroupDescGeneratorTest);

class AdminServiceImplTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void initZk();

protected:
    ZkWrapper *_zk;
    string _host;
};

void AdminServiceImplTest::setUp() { _zk = NULL; }

void AdminServiceImplTest::tearDown() { DELETE_AND_SET_NULL(_zk); }

void AdminServiceImplTest::initZk() {
    auto zkServer = zookeeper::ZkServer::getZkServer();
    unsigned short port = zkServer->port();
    ostringstream oss;
    oss << "127.0.0.1:" << port;
    _host = oss.str();
    _zk = new ZkWrapper(_host, 10);
    _zk->open();
}

TEST_F(AdminServiceImplTest, testCreate) {
    AdminServiceImpl serviceImpl;
    string nodePath = "zones.zone1";
    string nodeValue = "{\"a\":\"b\"}";
    CreateRequest request;
    request.set_nodepath(nodePath);
    request.set_nodevalue(nodeValue);
    Response response;
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());
    string jsonStr;
    ToString(serviceImpl._target, jsonStr);
    string expectStr = "{\n\"zones\":\n  {\n  \"zone1\":\n    {\n    \"a\":\n      \"b\"\n    }\n  }\n}";
    ASSERT_EQ(expectStr, jsonStr);
}

TEST_F(AdminServiceImplTest, testCreateFail) {
    AdminServiceImpl serviceImpl;
    {
        string nodePath = "zones.zone1.bb";
        string nodeValue = "{\"a\":\"b\"}";
        CreateRequest request;
        request.set_nodepath(nodePath);
        request.set_nodevalue(nodeValue);
        Response response;
        ASSERT_FALSE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
        ASSERT_TRUE(response.has_errorinfo());
    }
    {
        string nodePath = "zones.zone1";
        string nodeValue = "{\"a\":\"b\"},";
        CreateRequest request;
        request.set_nodepath(nodePath);
        request.set_nodevalue(nodeValue);
        Response response;
        ASSERT_FALSE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
        ASSERT_TRUE(response.has_errorinfo());
    }
}

TEST_F(AdminServiceImplTest, testUpdate) {
    AdminServiceImpl serviceImpl;
    string nodePath = "zones.zone1";
    string nodeValue = "{\"a\":\"b\"}";
    CreateRequest request;
    request.set_nodepath(nodePath);
    request.set_nodevalue(nodeValue);
    Response response;
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());

    nodePath = "zones.zone1.zone2";
    nodeValue = "{\"a\":\"b\"}";
    UpdateRequest uRequest;
    Response response1;
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);
    ASSERT_FALSE(AdminServiceImpl::doUpdate(&uRequest, &response1, serviceImpl._target));
    ASSERT_TRUE(response1.has_errorinfo());

    Response response2;
    nodePath = "zones.zone1";
    nodeValue = "{\"b\":\"c\"}";
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);
    ASSERT_TRUE(AdminServiceImpl::doUpdate(&uRequest, &response, serviceImpl._target));
    ASSERT_FALSE(response2.has_errorinfo());
    string jsonStr;
    ToString(serviceImpl._target, jsonStr);
    string expectStr = "{\n\"zones\":\n  {\n  \"zone1\":\n    {\n    \"b\":\n      \"c\"\n    }\n  }\n}";
    ASSERT_EQ(expectStr, jsonStr);
}

TEST_F(AdminServiceImplTest, testReadWithDetail) {
    AdminServiceImpl serviceImpl;
    auto roleManager = std::make_shared<StrictMockRoleManagerWrapper>();
    serviceImpl._roleManager = roleManager;

    string nodePath = "zones.zone1";
    string nodeValue = "{\"a\":\"b\"}";
    CreateRequest request;
    request.set_nodepath(nodePath);
    request.set_nodevalue(nodeValue);
    Response response;
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());

    nodePath = "systemInfo";
    ReadRequest rRequest;
    rRequest.set_nodepath(nodePath);
    ReadResponse rResponse;

    map<string, carbon::GroupStatus> groupStatuses;
    carbon::GroupStatus groupStatus;
    groupStatus.status = carbon::GroupStatus::RUNNING;
    map<string, RoleStatus> &roleStatuses = groupStatus.roleStatuses;
    RoleStatus roleStatus;
    roleStatus.set_rolename("role1");
    KVPair *kv = roleStatus.add_properties();
    kv->set_key("key1");
    kv->set_value("value1");
    RoleInstanceInfo *roleInstanceInfo = roleStatus.mutable_curinstanceinfo();
    roleInstanceInfo->set_roleid("role_id_1");
    roleInstanceInfo->set_targetcount(2);
    carbon::proto::ReplicaNode *replicaNode = roleInstanceInfo->add_replicanodes();
    replicaNode->set_custominfo("current_status");
    roleStatuses["role1"] = roleStatus;
    groupStatuses["zone1"] = groupStatus;

    EXPECT_CALL(*roleManager, getGroupStatuses(_, _)).WillOnce(SetArgReferee<1>(groupStatuses));
    ASSERT_TRUE(serviceImpl.doRead(&rRequest, &rResponse));
    ASSERT_FALSE(rResponse.has_errorinfo());
    string actValue = rResponse.nodevalue();
    map<string, carbon::GroupStatus> groupStatuses2;
    FastFromJsonString(groupStatuses2, actValue);
    carbon::GroupStatus &groupStatus2 = groupStatuses2["zone1"];
    ASSERT_EQ(groupStatus.status, groupStatus2.status);
    map<string, RoleStatus> &roleStatuses2 = groupStatus2.roleStatuses;
    RoleStatus &roleStatus2 = roleStatuses2["role1"];
    ASSERT_EQ(roleStatus.rolename(), roleStatus2.rolename());

    rRequest.Clear();
    rResponse.Clear();

    nodePath = "systemInfo.zone1.roleStatuses.role1";
    rRequest.set_nodepath(nodePath);
    EXPECT_CALL(*roleManager, getGroupStatuses(_, _)).WillOnce(SetArgReferee<1>(groupStatuses));
    ASSERT_TRUE(serviceImpl.doRead(&rRequest, &rResponse));
    ASSERT_FALSE(rResponse.has_errorinfo());
    actValue = rResponse.nodevalue();
    RoleStatus rs;
    ASSERT_TRUE(http_arpc::ProtoJsonizer::fromJsonString(actValue, &rs));
    ASSERT_EQ(roleStatus.rolename(), rs.rolename());

    request.set_nodepath("zones.zone1");
    request.set_nodevalue("{\"switch_mode\":\"column\",\"role_count\":2}");
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());
    groupStatuses.clear();
    groupStatuses["zone1.0"] = groupStatus;
    groupStatuses["zone1.1"] = groupStatus;

    nodePath = "systemInfo.zone1.roleStatuses.role1";
    rRequest.set_nodepath(nodePath);
    EXPECT_CALL(*roleManager, getGroupStatuses(_, _)).WillOnce(SetArgReferee<1>(groupStatuses));
    ASSERT_TRUE(serviceImpl.doRead(&rRequest, &rResponse));
    ASSERT_FALSE(rResponse.has_errorinfo());
    RoleStatus rs2;
    ASSERT_TRUE(http_arpc::ProtoJsonizer::fromJsonString(rResponse.nodevalue(), &rs2));
    ASSERT_EQ(roleStatus.rolename(), rs2.rolename());

    serviceImpl._roleManager = NULL;
}

TEST_F(AdminServiceImplTest, testReleaseSlot) {
    AdminServiceImpl serviceImpl;
    auto roleManager = std::make_shared<StrictMockRoleManagerWrapper>();
    serviceImpl._roleManager = roleManager;
    serviceImpl._serviceReady = true;

    ::google::protobuf::RpcController *controller = NULL;
    ::google::protobuf::Closure *done = NULL;
    {
        ReleaseSlotRequest request;
        Response response;
        EXPECT_CALL(*roleManager, releseSlots(_, _)).WillOnce(Return(false));
        serviceImpl.releaseSlot(controller, &request, &response, done);
        EXPECT_TRUE(response.has_errorinfo());
    }
    {
        ReleaseSlotRequest request;
        Response response;
        EXPECT_CALL(*roleManager, releseSlots(_, _)).WillOnce(Return(true));
        serviceImpl.releaseSlot(controller, &request, &response, done);
        EXPECT_FALSE(response.has_errorinfo());
    }
    serviceImpl._roleManager = NULL;
}

TEST_F(AdminServiceImplTest, testRead) {
    AdminServiceImpl serviceImpl;

    string nodePath = "zones.zone1";
    string nodeValue = "{\"a\":\"b\"}";
    CreateRequest request;
    request.set_nodepath(nodePath);
    request.set_nodevalue(nodeValue);
    Response response;
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());

    nodePath = "zones.zone1";
    ReadRequest rRequest;
    rRequest.set_nodepath(nodePath);
    ReadResponse rResponse;

    ASSERT_TRUE(serviceImpl.doRead(&rRequest, &rResponse));
    ASSERT_FALSE(rResponse.has_errorinfo());
    string actValue = rResponse.nodevalue();
    ASSERT_EQ("{\"a\":\"b\"}", actValue);

    nodePath = "zones.zone1.a";
    rRequest.set_nodepath(nodePath);
    ReadResponse rResponse1;
    ASSERT_TRUE(serviceImpl.doRead(&rRequest, &rResponse1));
    ASSERT_FALSE(rResponse1.has_errorinfo());
    actValue = rResponse1.nodevalue();
    ASSERT_EQ("\"b\"", actValue);

    nodePath = "zones.zone1.b";
    rRequest.set_nodepath(nodePath);
    ReadResponse rResponse2;
    ASSERT_FALSE(serviceImpl.doRead(&rRequest, &rResponse2));
    ASSERT_TRUE(rResponse2.has_errorinfo());
}

TEST_F(AdminServiceImplTest, testDelete) {
    AdminServiceImpl serviceImpl;
    string nodePath = "zones.zone1";
    string nodeValue = "{\"a\":\"b\"}";
    CreateRequest request;
    request.set_nodepath(nodePath);
    request.set_nodevalue(nodeValue);
    Response response;
    ASSERT_TRUE(AdminServiceImpl::doCreate(&request, &response, serviceImpl._target));
    ASSERT_FALSE(response.has_errorinfo());

    nodePath = "zones.zone1";
    DeleteRequest dRequest;
    dRequest.set_nodepath(nodePath);
    Response dResponse;
    ASSERT_TRUE(AdminServiceImpl::doDel(&dRequest, &response, serviceImpl._target));
    ASSERT_FALSE(dResponse.has_errorinfo());

    nodePath = "zones.zone1";
    ReadRequest rRequest;
    rRequest.set_nodepath(nodePath);
    ReadResponse rResponse;
    serviceImpl.doRead(&rRequest, &rResponse);
    ASSERT_TRUE(rResponse.has_errorinfo());
    string actValue = rResponse.nodevalue();
    ASSERT_EQ("", actValue);

    nodePath = "zones.zone1.a";
    dRequest.set_nodepath(nodePath);
    Response dResponse1;
    ASSERT_FALSE(AdminServiceImpl::doDel(&dRequest, &dResponse1, serviceImpl._target));
    ASSERT_TRUE(dResponse1.has_errorinfo());
}

TEST_F(AdminServiceImplTest, testBatch) {
    AdminServiceImpl serviceImpl;
    string nodePath1 = "zones.zone1";
    string nodeValue1 = "{\"a\":\"b\"}";
    string nodePath2 = "zones.zone2";
    string nodeValue2 = "{\"aa\":\"bb\"}";
    {
        BatchRequest batchRequest;
        CreateRequest *cRequest = batchRequest.add_createrequests();
        cRequest->set_nodepath(nodePath1);
        cRequest->set_nodevalue(nodeValue1);
        cRequest = batchRequest.add_createrequests();
        cRequest->set_nodepath(nodePath2);
        cRequest->set_nodevalue(nodeValue2);
        Response response;
        ASSERT_TRUE(AdminServiceImpl::doBatch(&batchRequest, &response, serviceImpl._target));
        ASSERT_FALSE(response.has_errorinfo());

        ReadRequest rRequest;
        rRequest.set_nodepath(nodePath1);
        ReadResponse rResponse;
        serviceImpl.doRead(&rRequest, &rResponse);
        ASSERT_FALSE(rResponse.has_errorinfo());
        string actValue = rResponse.nodevalue();
        ASSERT_EQ("{\"a\":\"b\"}", actValue);
    }
    {
        BatchRequest batchRequest;
        DeleteRequest *dRequest = batchRequest.add_deleterequests();
        dRequest->set_nodepath(nodePath1);
        UpdateRequest *uRequest = batchRequest.add_updaterequests();
        uRequest->set_nodepath(nodePath2);
        nodeValue2 = "{\"aa\":\"bbb\"}";
        uRequest->set_nodevalue(nodeValue2);
        Response response;
        ASSERT_TRUE(AdminServiceImpl::doBatch(&batchRequest, &response, serviceImpl._target));
        ASSERT_FALSE(response.has_errorinfo());
        {
            ReadRequest rRequest;
            rRequest.set_nodepath(nodePath1);
            ReadResponse rResponse;
            serviceImpl.doRead(&rRequest, &rResponse);
            ASSERT_TRUE(rResponse.has_errorinfo());
            string actValue = rResponse.nodevalue();
            ASSERT_EQ("", actValue);
        }
        {
            ReadRequest rRequest;
            rRequest.set_nodepath(nodePath2);
            ReadResponse rResponse;
            serviceImpl.doRead(&rRequest, &rResponse);
            ASSERT_FALSE(rResponse.has_errorinfo());
            string actValue = rResponse.nodevalue();
            ASSERT_EQ("{\"aa\":\"bbb\"}", actValue);
        }
    }
}

TEST_F(AdminServiceImplTest, testDeserializeWithStatusFileAbsent) {
    initZk();
    AdminServiceImpl serviceImpl;
    string adminStatus = "zfs://" + _host + "/admin_status_not_exist";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);
    JsonMap target;
    std::string targetStr;
    ASSERT_TRUE(serviceImpl.deserialize(target, targetStr));
}

TEST_F(AdminServiceImplTest, testSerialize) {
    initZk();
    AdminServiceImpl serviceImpl;
    string adminStatus = "zfs://" + _host + "/admin_status";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);

    JsonMap jsonMap;
    string value = "{\"a\":\"b\"}";
    Any any;
    try {
        FastFromJsonString(any, value);
    } catch (const autil::legacy::ExceptionBase &e) {
        string errMsg = "to json failed";
        AUTIL_LOG(WARN, "%s, %s", errMsg.c_str(), e.what());
    }
    jsonMap["a"] = any;
    ASSERT_TRUE(serviceImpl.serialize(FastToJsonString(jsonMap)));
    JsonMap target = jsonMap;
    jsonMap.clear();
    std::string targetStr;
    ASSERT_TRUE(serviceImpl.deserialize(jsonMap, targetStr));
    ASSERT_EQ(size_t(1), jsonMap.size());
    ASSERT_EQ(targetStr, FastToJsonString(jsonMap));
    ASSERT_TRUE(Equal(jsonMap, target));
}

TEST_F(AdminServiceImplTest, testMultiThread) {
    initZk();
    AdminServiceImpl serviceImpl;
    serviceImpl._serviceReady = true;
    auto adminStatus = "zfs://" + _host + "/admin_status";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);
    string testPath = GET_TEST_DATA_PATH() + "admin_test/";
    string testFile = testPath + "big.json";
    ASSERT_TRUE(TargetLoader::loadFromFile(testFile, serviceImpl._target));
    string nodePath = "zones.cons_52.app_info.keep_count";
    string nodeValue = "10";
    UpdateRequest uRequest;
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);
    CreateRequest cRequest;
    cRequest.set_nodepath(nodePath);
    cRequest.set_nodevalue(nodeValue);
    DeleteRequest dRequest;
    dRequest.set_nodepath(nodePath);
    std::thread updateThread([uRequest, &serviceImpl]() {
        Response response;
        for (int i = 0; i < 100; i++) {
            serviceImpl.update(NULL, &uRequest, &response, NULL);
        }
    });
    std::thread createThread([cRequest, &serviceImpl]() {
        Response response;
        for (int i = 0; i < 100; i++) {
            serviceImpl.create(NULL, &cRequest, &response, NULL);
        }
    });
    std::thread deleteThread([dRequest, &serviceImpl]() {
        Response response;
        for (int i = 0; i < 100; i++) {
            serviceImpl.del(NULL, &dRequest, &response, NULL);
        }
    });
    updateThread.join();
    createThread.join();
    deleteThread.join();
}

TEST_F(AdminServiceImplTest, testReRangeGroupStatus) {
    map<string, carbon::GroupStatus> groupStatusMap;
    groupStatusMap["g1"].roleStatuses["r1"];
    groupStatusMap["g2"].roleStatuses["r2"];
    groupStatusMap["g3.1"].roleStatuses["r1"];
    groupStatusMap["g3.2"].roleStatuses["r2"];
    AdminServiceImpl::reRangeGroupStatus(groupStatusMap);
    EXPECT_EQ(3, groupStatusMap.size());
    EXPECT_TRUE(groupStatusMap.find("g3") != groupStatusMap.end());
    EXPECT_EQ(2, groupStatusMap["g3"].roleStatuses.size());
}

TEST_F(AdminServiceImplTest, testGetCommitError) {
    AdminServiceImpl serviceImpl;
    serviceImpl.setCommitError("abc");
    ReadRequest rRequest;
    rRequest.set_nodepath("error");
    ReadResponse rResponse;
    serviceImpl.doRead(&rRequest, &rResponse);
    ASSERT_FALSE(rResponse.has_errorinfo());
    ASSERT_EQ("\"abc\"", rResponse.nodevalue());
}

TEST_F(AdminServiceImplTest, testJsonize) {
    string testPath = GET_TEST_DATA_PATH() + "admin_test/";
    string testFile = testPath + "target.json";
    JsonMap t;
    TimeRecorder tr;
    ASSERT_TRUE(TargetLoader::loadFromFile(testFile, t));
    tr.recordAndReset("from string");
    JsonMap tt;
    tt = JsonNodeRef::copy(t);
    tr.recordAndReset("copy");
}

TEST_F(AdminServiceImplTest, testPerfRead) {
    initZk();
    AdminServiceImpl serviceImpl;
    serviceImpl._serviceReady = true;
    serviceImpl._roleManager = std::make_shared<carbon::compatible::RoleManagerWrapperImpl>();
    auto adminStatus = "zfs://" + _host + "/admin_status";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);
    string testPath = GET_TEST_DATA_PATH() + "admin_test/";
    string testFile = testPath + "big.json";
    ASSERT_TRUE(TargetLoader::loadFromFile(testFile, serviceImpl._target));
    string nodePath = "zones.cons_52.app_info.keep_count";
    string nodeValue = "10";
    ReadRequest rRequest;
    rRequest.set_nodepath("systemInfo.cons_52");

    UpdateRequest uRequest;
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);

    std::thread readThread([rRequest, &serviceImpl]() {
        ReadResponse response;
        for (int i = 0; i < 10; i++) {
            serviceImpl.read(NULL, &rRequest, &response, NULL);
        }
        int64_t startTime = TimeUtility::currentTime();
        int count = 100;
        for (int i = 0; i < count; i++) {
            serviceImpl.read(NULL, &rRequest, &response, NULL);
        }
        int64_t endTime = TimeUtility::currentTime();
        int64_t readAvgTime = (endTime - startTime) / (1000 * count);
        cout << "readAvgTime:" << readAvgTime << "ms" << endl;
    });
    std::thread updateThread([uRequest, &serviceImpl]() {
        Response response;
        int64_t startTime = TimeUtility::currentTime();
        int updateCount = 80;
        for (int i = 0; i < updateCount; i++) {
            serviceImpl.update(NULL, &uRequest, &response, NULL);
        }
        int64_t endTime = TimeUtility::currentTime();
        int64_t readAvgTime = (endTime - startTime) / (1000 * updateCount);
        cout << "updateAvgTime:" << readAvgTime << "ms" << endl;
    });

    readThread.join();
    updateThread.join();
}

TEST_F(AdminServiceImplTest, testPerfMultiRead) {
    initZk();
    AdminServiceImpl serviceImpl;
    serviceImpl._serviceReady = true;
    serviceImpl._roleManager = std::make_shared<carbon::compatible::RoleManagerWrapperImpl>();
    auto adminStatus = "zfs://" + _host + "/admin_status";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);
    string testPath = GET_TEST_DATA_PATH() + "admin_test/";
    string testFile = testPath + "big.json";
    ASSERT_TRUE(TargetLoader::loadFromFile(testFile, serviceImpl._target));
    string nodePath = "zones.cons_52.app_info.keep_count";
    string nodeValue = "10";
    ReadRequest rRequest;
    rRequest.set_nodepath("zones.cons_52");

    UpdateRequest uRequest;
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);

    int readThreadSize = 5;
    std::thread readThreads[readThreadSize];
    for (int i = 0; i < readThreadSize; i++) {
        readThreads[i] = thread([rRequest, &serviceImpl]() {
            ReadResponse response;
            for (int i = 0; i < 10; i++) {
                serviceImpl.read(NULL, &rRequest, &response, NULL);
            }
            int64_t startTime = TimeUtility::currentTime();
            int count = 50;
            for (int i = 0; i < count; i++) {
                serviceImpl.read(NULL, &rRequest, &response, NULL);
            }
            int64_t endTime = TimeUtility::currentTime();
            int64_t readAvgTime = (endTime - startTime) / (1000 * count);
            cout << "readAvgTime:" << readAvgTime << "ms" << endl;
        });
    }

    std::thread updateThread([uRequest, &serviceImpl]() {
        Response response;
        int64_t startTime = TimeUtility::currentTime();
        int updateCount = 30;
        for (int i = 0; i < updateCount; i++) {
            serviceImpl.update(NULL, &uRequest, &response, NULL);
        }
        int64_t endTime = TimeUtility::currentTime();
        int64_t readAvgTime = (endTime - startTime) / (1000 * updateCount);
        cout << "updateAvgTime:" << readAvgTime << "ms" << endl;
    });

    for (int i = 0; i < readThreadSize; i++) {
        readThreads[i].join();
    }
    updateThread.join();
}

TEST_F(AdminServiceImplTest, testPerfMultiReadBig) {
    initZk();
    AdminServiceImpl serviceImpl;
    serviceImpl._serviceReady = true;
    serviceImpl._roleManager = std::make_shared<carbon::compatible::RoleManagerWrapperImpl>();
    auto adminStatus = "zfs://" + _host + "/admin_status";
    serviceImpl._state = std::make_unique<worker_framework::ZkState>(_zk, adminStatus);
    string testPath = GET_TEST_DATA_PATH() + "admin_test/";
    string testFile = testPath + "bigbig.json";
    ASSERT_TRUE(TargetLoader::loadFromFile(testFile, serviceImpl._target));
    string nodePath = "zones.cons_52.app_info.keep_count";
    string nodeValue = "10";
    ReadRequest rRequest;
    rRequest.set_nodepath("systemInfo.cons_52");

    UpdateRequest uRequest;
    uRequest.set_nodepath(nodePath);
    uRequest.set_nodevalue(nodeValue);
    int readThreadSize = 1;
    std::thread readThreads[readThreadSize];
    for (int i = 0; i < readThreadSize; i++) {
        readThreads[i] = thread([rRequest, &serviceImpl]() {
            ReadResponse response;
            for (int i = 0; i < 10; i++) {
                serviceImpl.read(NULL, &rRequest, &response, NULL);
            }
            int64_t startTime = TimeUtility::currentTime();
            int count = 50;
            for (int i = 0; i < count; i++) {
                serviceImpl.read(NULL, &rRequest, &response, NULL);
            }
            int64_t endTime = TimeUtility::currentTime();
            int64_t readAvgTime = (endTime - startTime) / (1000 * count);
            cout << "readAvgTime:" << readAvgTime << "ms" << endl;
        });
    }

    // std::thread updateThread([uRequest, &serviceImpl](){
    //             Response response;
    //             int64_t startTime = TimeUtility::currentTime();
    //             int updateCount = 20;
    //             for (int i = 0; i < updateCount; i++) {
    //                 serviceImpl.update(NULL, &uRequest, &response, NULL);
    //             }
    //             int64_t endTime = TimeUtility::currentTime();
    //             int64_t updateAvgTime= (endTime-startTime)/(1000*updateCount);
    //             cout << "updateAvgTime:" << updateAvgTime << "ms" << endl;
    //         });

    for (int i = 0; i < readThreadSize; i++) {
        readThreads[i].join();
    }
    // updateThread.join();
}

TEST_F(AdminServiceImplTest, testSimplifyGroupsStatus) {
    map<string, carbon::GroupStatus> groupStatusMap;
    carbon::proto::RoleStatus roleStatus;
    auto curRoleInfo = roleStatus.mutable_curinstanceinfo();
    for (size_t i = 0; i < 10; i++) {
        auto replicaNode = curRoleInfo->add_replicanodes();
        replicaNode->set_targetcustominfo("target_custom_info");
        replicaNode->set_custominfo("custom_info");
        replicaNode->set_userdefversion("123");
    }

    auto nextRoleInfo = roleStatus.mutable_nextinstanceinfo();
    for (size_t i = 0; i < 10; i++) {
        auto replicaNode = nextRoleInfo->add_replicanodes();
        replicaNode->set_targetcustominfo("target_custom_info");
        replicaNode->set_custominfo("custom_info");
        replicaNode->set_userdefversion("124");
    }

    groupStatusMap["g1"].roleStatuses["r1"] = roleStatus;
    AdminServiceImpl::simplifyGroupsStatus(groupStatusMap);
    auto simplifiedRoleStatus = groupStatusMap["g1"].roleStatuses["r1"];
    auto newCurRoleInfo = simplifiedRoleStatus.curinstanceinfo();
    ASSERT_EQ(curRoleInfo->replicanodes_size(), newCurRoleInfo.replicanodes_size());
    for (size_t i = 0; i < newCurRoleInfo.replicanodes_size(); i++) {
        const auto &node = curRoleInfo->replicanodes(i);
        const auto &newNode = newCurRoleInfo.replicanodes(i);
        ASSERT_EQ(node.userdefversion(), newNode.userdefversion());
        ASSERT_FALSE(newNode.has_targetcustominfo());
        ASSERT_FALSE(newNode.has_custominfo());
    }

    auto newNextRoleInfo = simplifiedRoleStatus.nextinstanceinfo();
    ASSERT_EQ(nextRoleInfo->replicanodes_size(), newNextRoleInfo.replicanodes_size());
    for (size_t i = 0; i < newNextRoleInfo.replicanodes_size(); i++) {
        const auto &node = nextRoleInfo->replicanodes(i);
        const auto &newNode = newNextRoleInfo.replicanodes(i);
        ASSERT_EQ(node.userdefversion(), newNode.userdefversion());
        ASSERT_FALSE(newNode.has_targetcustominfo());
        ASSERT_FALSE(newNode.has_custominfo());
    }
}

} // namespace suez
