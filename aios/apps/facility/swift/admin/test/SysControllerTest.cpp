#include "swift/admin/SysController.h"

#include <algorithm>
#include <assert.h>
#include <atomic>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <fstream> // IWYU pragma: keep
#include <functional>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdlib.h>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>
#include <zlib.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/DllWrapper.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/CleanAtDeleteManager.h"
#include "swift/admin/ModuleManager.h"
#include "swift/admin/RoleInfo.h"
#include "swift/admin/TopicInStatusManager.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/WorkerManager.h"
#include "swift/admin/WorkerManagerLocal.h"
#include "swift/admin/WorkerTable.h"
#include "swift/admin/modules/CleanDataModule.h"
#include "swift/admin/modules/DealErrorBrokerModule.h"
#include "swift/admin/modules/NoUseTopicInfo.h"
#include "swift/admin/modules/NoUseTopicModule.h"
#include "swift/admin/modules/TopicAclManageModule.h"
#include "swift/admin/test/MockSysController.h"
#include "swift/auth/RequestAuthenticator.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/auth/TopicAclRequestHandler.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/config/ConfigVersionManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/CircularVector.h"
#include "swift/util/LogicTopicHelper.h"
#include "swift/util/PanguInlineFileUtil.h"
#include "swift/util/TargetRecorder.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {
class ZkDataAccessor;
} // namespace util
} // namespace swift

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace swift::common;
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::config;

namespace swift {
namespace admin {

class SysControllerTest : public TESTBASE {
public:
    void setUp() override {
        _zkServer = zookeeper::ZkServer::getZkServer();
        std::ostringstream oss;
        oss << "127.0.0.1:" << _zkServer->port();
        _zkRoot = "zfs://" + oss.str() + "/";
        cout << "zkRoot: " << _zkRoot << endl;

        _adminConfig.dfsRoot = GET_TEMPLATE_DATA_PATH();
        _adminConfig.zkRoot = _zkRoot;
        _sysController = new SysController(&_adminConfig, nullptr);
        ASSERT_TRUE(_sysController->init());

        _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
        ASSERT_TRUE(_zkWrapper->open());

        ASSERT_TRUE(_sysController->_zkDataAccessor->init(_zkWrapper, _zkRoot));

        _leaderInfoConst.set_address("127.0.0.1");
        _leaderInfoConst.set_sysstop(false);
        _leaderInfoConst.set_starttimesec(13579);
        _leaderInfoConst.set_selfmasterversion(246810);

        PanguInlineFileUtil::setInlineFileMock(false);
        _sysController->_moduleManager = std::make_unique<ModuleManager>();
        _sysController->_moduleManager->init(&(_sysController->_adminStatusManager), _sysController, &_adminConfig);
    }

    void tearDown() override {
        DELETE_AND_SET_NULL(_sysController);
        DELETE_AND_SET_NULL(_zkWrapper);
    }
    void getAllTopicInfo();

private:
    string getExtendDfsRoot(const TopicCreationRequest &meta);
    void
    assertReserveCount(uint32_t inputCount, uint32_t reserveCount, uint32_t expectCount, uint32_t expectStartIndex);
    void assertGetSchema(bool isFollower);

protected:
    AdminConfig _adminConfig;
    ZkDataAccessor *_zkDataAccessor = nullptr;
    SysController *_sysController = nullptr;
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
    LeaderInfo _leaderInfoConst;
};

TEST_F(SysControllerTest, testCreateTopic) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_ADMIN_NOT_LEADER, topicCreateResponse.errorinfo().errcode());

    _sysController->setPrimary(true);
    topicCreateRequest.set_partitioncount(6);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
    for (int32_t i = 0; i < 3; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        int64_t createTime = 0;
        topicCreateRequest.set_topicname(topicName);
        if (1 == i) {
            topicCreateRequest.set_topicmode(TOPIC_MODE_SECURITY);
            topicCreateRequest.set_needfieldfilter(true);
            topicCreateRequest.set_obsoletefiletimeinterval(1);
            topicCreateRequest.set_reservedfilecount(2);
            topicCreateRequest.set_deletetopicdata(true);
            topicCreateRequest.set_partitionminbuffersize(32);
            topicCreateRequest.set_partitionmaxbuffersize(64);
            topicCreateRequest.set_topicexpiredtime(11);
            topicCreateRequest.set_dfsroot("");
            topicCreateRequest.add_owners("Jack");
            topicCreateRequest.set_readsizelimitsec(100);
            topicCreateRequest.set_enablelongpolling(true);
        } else if (2 == i) {
            topicCreateRequest.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
            topicCreateRequest.set_dfsroot("");
            topicCreateRequest.add_owners("Smith");
        } else {
            topicCreateRequest.set_dfsroot(_adminConfig.dfsRoot + "/user");
        }
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.resource(), topicCreateRequest.resource());
        ASSERT_EQ(meta.partitionlimit(), topicCreateRequest.partitionlimit());
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, meta.topicmode());
            ASSERT_TRUE(meta.needfieldfilter());
            ASSERT_EQ((int64_t)1, meta.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, meta.reservedfilecount());
            ASSERT_FALSE(meta.deletetopicdata());
            ASSERT_EQ(uint32_t(32), meta.partitionminbuffersize());
            ASSERT_EQ(uint32_t(256), meta.partitionmaxbuffersize()); // modify partition size
            ASSERT_EQ(int64_t(11), meta.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, meta.dfsroot());
            ASSERT_EQ(1, meta.owners_size());
            ASSERT_EQ("Jack", meta.owners(0));
            ASSERT_EQ(100, meta.readsizelimitsec());
            ASSERT_EQ(true, meta.enablelongpolling());
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, meta.topicmode());
            ASSERT_EQ(string(""), meta.dfsroot());
            ASSERT_EQ(2, meta.owners_size());
            ASSERT_EQ("Jack", meta.owners(0));
            ASSERT_EQ("Smith", meta.owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, meta.topicmode());
            ASSERT_TRUE(!meta.needfieldfilter());
            ASSERT_EQ((int64_t)-1, meta.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, meta.reservedfilecount());
            ASSERT_FALSE(meta.deletetopicdata());
            ASSERT_EQ(uint32_t(8), meta.partitionminbuffersize());
            ASSERT_EQ(uint32_t(256), meta.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), meta.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", meta.dfsroot());
            ASSERT_EQ(0, meta.owners_size());
        }
        createTime = meta.createtime();
        ASSERT_TRUE(createTime > 0);

        TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
        ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
        ASSERT_EQ(topicCreateRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
        ASSERT_EQ(topicCreateRequest.resource(), topicInfoPtr->getTopicMeta().resource());
        ASSERT_EQ(topicCreateRequest.partitionlimit(), topicInfoPtr->getTopicMeta().partitionlimit());
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)32, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
            ASSERT_EQ(int64_t(11), topicInfoPtr->getTopicMeta().topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(1, topicInfoPtr->getTopicMeta().owners_size());
            ASSERT_EQ("Jack", topicInfoPtr->getTopicMeta().owners(0));
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_EQ(string(""), topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(2, topicInfoPtr->getTopicMeta().owners_size());
            ASSERT_EQ("Jack", topicInfoPtr->getTopicMeta().owners(0));
            ASSERT_EQ("Smith", topicInfoPtr->getTopicMeta().owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(!topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)-1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)8, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), topicInfoPtr->getTopicMeta().topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(0, topicInfoPtr->getTopicMeta().owners_size());
        }

        TopicInfoRequest topicInfoRequest;
        topicInfoRequest.set_topicname(topicName);
        TopicInfoResponse topicInfoResponse;
        _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
        protocol::TopicInfo ti = topicInfoResponse.topicinfo();
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, ti.topicmode());
            ASSERT_TRUE(ti.needfieldfilter());
            ASSERT_EQ((int64_t)1, ti.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, ti.reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)32, ti.partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, ti.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(11), ti.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, ti.dfsroot());
            ASSERT_EQ(1, ti.owners_size());
            ASSERT_EQ("Jack", ti.owners(0));
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, ti.topicmode());
            ASSERT_EQ(string(""), ti.dfsroot());
            ASSERT_EQ(2, ti.owners_size());
            ASSERT_EQ("Jack", ti.owners(0));
            ASSERT_EQ("Smith", ti.owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, ti.topicmode());
            ASSERT_TRUE(!ti.needfieldfilter());
            ASSERT_EQ((int64_t)-1, ti.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, ti.reservedfilecount());
            ASSERT_FALSE(ti.deletetopicdata());
            ASSERT_EQ((uint32_t)8, ti.partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, ti.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), ti.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", ti.dfsroot());
            ASSERT_EQ(0, ti.owners_size());
        }
        ASSERT_EQ(createTime, ti.createtime());
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, topicCreateResponse.errorinfo().errcode());
    }

    string topicName = "topic_b";
    _adminConfig.dfsRoot = "";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_topicmode(TOPIC_MODE_SECURITY);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
}

TEST_F(SysControllerTest, testCreateTopicBatch) {
    TopicBatchCreationRequest topicCreateRequest;
    TopicBatchCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    for (int32_t i = 0; i < 3; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        TopicCreationRequest *meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname(topicName);
        meta->set_partitioncount(i + 1);
        if (1 == i) {
            meta->set_topicmode(TOPIC_MODE_SECURITY);
            meta->set_needfieldfilter(true);
            meta->set_obsoletefiletimeinterval(1);
            meta->set_reservedfilecount(2);
            meta->set_deletetopicdata(true);
            meta->set_partitionminbuffersize(32);
            meta->set_partitionmaxbuffersize(64);
            meta->set_topicexpiredtime(11);
            meta->set_dfsroot("");
            meta->add_owners("Jack");
        } else if (2 == i) {
            meta->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
            meta->set_dfsroot("");
            meta->add_owners("Jack");
            meta->add_owners("Smith");
        } else {
            meta->set_dfsroot(_adminConfig.dfsRoot + "/user");
        }
    }

    _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, topicCreateResponse.errorinfo().errcode());
    EXPECT_EQ("ERROR_ADMIN_TOPIC_HAS_EXISTED[topic_0;topic_1;topic_2]", topicCreateResponse.errorinfo().errmsg());

    for (int32_t i = 0; i < 3; ++i) {
        TopicCreationRequest oneRequest = topicCreateRequest.topicrequests(i);
        TopicCreationRequest meta;
        string topicName = "topic_" + StringUtil::toString(i);
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), oneRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), oneRequest.partitioncount());
        ASSERT_EQ(meta.resource(), oneRequest.resource());
        ASSERT_EQ(meta.partitionlimit(), oneRequest.partitionlimit());
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, meta.topicmode());
            ASSERT_TRUE(meta.needfieldfilter());
            ASSERT_EQ((int64_t)1, meta.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, meta.reservedfilecount());
            ASSERT_FALSE(meta.deletetopicdata());
            ASSERT_EQ(uint32_t(32), meta.partitionminbuffersize());
            ASSERT_EQ(uint32_t(256), meta.partitionmaxbuffersize()); // modify partition size
            ASSERT_EQ(int64_t(11), meta.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, meta.dfsroot());
            ASSERT_EQ(1, meta.owners_size());
            ASSERT_EQ("Jack", meta.owners(0));
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, meta.topicmode());
            ASSERT_EQ(string(""), meta.dfsroot());
            ASSERT_EQ(2, meta.owners_size());
            ASSERT_EQ("Jack", meta.owners(0));
            ASSERT_EQ("Smith", meta.owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, meta.topicmode());
            ASSERT_TRUE(!meta.needfieldfilter());
            ASSERT_EQ((int64_t)-1, meta.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, meta.reservedfilecount());
            ASSERT_FALSE(meta.deletetopicdata());
            ASSERT_EQ(uint32_t(8), meta.partitionminbuffersize());
            ASSERT_EQ(uint32_t(256), meta.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), meta.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", meta.dfsroot());
            ASSERT_EQ(0, meta.owners_size());
        }

        int64_t createTime = meta.createtime();
        ASSERT_TRUE(createTime > 0);

        TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
        ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
        ASSERT_EQ(oneRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
        ASSERT_EQ(oneRequest.resource(), topicInfoPtr->getTopicMeta().resource());
        ASSERT_EQ(oneRequest.partitionlimit(), topicInfoPtr->getTopicMeta().partitionlimit());
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)32, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
            ASSERT_EQ(int64_t(11), topicInfoPtr->getTopicMeta().topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(1, topicInfoPtr->getTopicMeta().owners_size());
            ASSERT_EQ("Jack", topicInfoPtr->getTopicMeta().owners(0));
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_EQ(string(""), topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(2, topicInfoPtr->getTopicMeta().owners_size());
            ASSERT_EQ("Jack", topicInfoPtr->getTopicMeta().owners(0));
            ASSERT_EQ("Smith", topicInfoPtr->getTopicMeta().owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(!topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)-1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)8, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), topicInfoPtr->getTopicMeta().topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", topicInfoPtr->getTopicMeta().dfsroot());
            ASSERT_EQ(0, topicInfoPtr->getTopicMeta().owners_size());
        }

        TopicInfoRequest topicInfoRequest;
        topicInfoRequest.set_topicname(topicName);
        TopicInfoResponse topicInfoResponse;
        _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
        protocol::TopicInfo ti = topicInfoResponse.topicinfo();
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, ti.topicmode());
            ASSERT_TRUE(ti.needfieldfilter());
            ASSERT_EQ((int64_t)1, ti.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, ti.reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)32, ti.partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, ti.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(11), ti.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot, ti.dfsroot());
            ASSERT_EQ(1, ti.owners_size());
            ASSERT_EQ("Jack", ti.owners(0));
        } else if (2 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, ti.topicmode());
            ASSERT_EQ(string(""), ti.dfsroot());
            ASSERT_EQ(2, ti.owners_size());
            ASSERT_EQ("Jack", ti.owners(0));
            ASSERT_EQ("Smith", ti.owners(1));
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, ti.topicmode());
            ASSERT_TRUE(!ti.needfieldfilter());
            ASSERT_EQ((int64_t)-1, ti.obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, ti.reservedfilecount());
            ASSERT_FALSE(ti.deletetopicdata());
            ASSERT_EQ((uint32_t)8, ti.partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, ti.partitionmaxbuffersize());
            ASSERT_EQ(int64_t(-1), ti.topicexpiredtime());
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", ti.dfsroot());
            ASSERT_EQ(0, ti.owners_size());
        }
        ASSERT_EQ(createTime, ti.createtime());

        TopicBatchCreationRequest createReq;
        TopicBatchCreationResponse createRes;
        TopicCreationRequest *req = createReq.add_topicrequests();
        *req = oneRequest;
        _sysController->createTopicBatch(&createReq, &createRes);
        ASSERT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, createRes.errorinfo().errcode());
        EXPECT_EQ("ERROR_ADMIN_TOPIC_HAS_EXISTED[topic_" + std::to_string(i) + "]", createRes.errorinfo().errmsg());
    }

    // test ignore exist topics
    TopicBatchCreationRequest batchCreateReq;
    TopicBatchCreationResponse batchCreateRes;
    TopicCreationRequest meta;
    meta.set_partitioncount(1);
    for (int i = 0; i < 3; ++i) {
        meta.set_topicname("topic_" + to_string(i));
        *batchCreateReq.add_topicrequests() = meta;
    }
    _sysController->createTopicBatch(&batchCreateReq, &batchCreateRes);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, batchCreateRes.errorinfo().errcode());
    EXPECT_EQ("ERROR_ADMIN_TOPIC_HAS_EXISTED[topic_0;topic_1;topic_2]", batchCreateRes.errorinfo().errmsg());

    batchCreateReq.set_ignoreexist(true);
    _sysController->createTopicBatch(&batchCreateReq, &batchCreateRes);
    ASSERT_EQ(ERROR_NONE, batchCreateRes.errorinfo().errcode());
    // not affect exists topics
    ASSERT_EQ(1, _sysController->_topicTable._topicMap["topic_0"]->getPartitionCount());
    ASSERT_EQ(2, _sysController->_topicTable._topicMap["topic_1"]->getPartitionCount());
    ASSERT_EQ(3, _sysController->_topicTable._topicMap["topic_2"]->getPartitionCount());

    meta.set_topicname("topic_3");
    meta.set_partitioncount(4);
    *batchCreateReq.add_topicrequests() = meta;
    _sysController->createTopicBatch(&batchCreateReq, &batchCreateRes);
    ASSERT_EQ(ERROR_NONE, batchCreateRes.errorinfo().errcode());
    TopicInfoPtr info = _sysController->_topicTable.findTopic("topic_3");
    ASSERT_EQ(4, info->getPartitionCount());

    // same topic batch create again, ignore same
    batchCreateReq.Clear();
    batchCreateReq.set_ignoreexist(true);
    for (int i = 0; i < 4; i++) {
        meta.set_topicname("topic_" + to_string(i));
        meta.set_partitioncount(100);
        *batchCreateReq.add_topicrequests() = meta;
    }
    _sysController->createTopicBatch(&batchCreateReq, &batchCreateRes);
    ASSERT_EQ(ERROR_NONE, batchCreateRes.errorinfo().errcode());
    ASSERT_EQ(4, _sysController->_topicTable._topicMap.size());
    ASSERT_EQ(1, _sysController->_topicTable._topicMap["topic_0"]->getPartitionCount());
    ASSERT_EQ(2, _sysController->_topicTable._topicMap["topic_1"]->getPartitionCount());
    ASSERT_EQ(3, _sysController->_topicTable._topicMap["topic_2"]->getPartitionCount());
    ASSERT_EQ(4, _sysController->_topicTable._topicMap["topic_3"]->getPartitionCount());
}

TEST_F(SysControllerTest, testCreateTopicDeleteTopicWithAclChange) {
    _sysController->setPrimary(true);
    _sysController->_moduleManager->loadModule<TopicAclManageModule>();
    auto topicAclModule = _sysController->_moduleManager->getModule<TopicAclManageModule>();
    topicAclModule->update(_sysController->_adminStatusManager._currentStatus);
    auto dataManager = topicAclModule->_requestAuthenticator->_topicAclRequestHandler->_topicAclDataManager;
    EXPECT_EQ(0, dataManager->_topicAclDataMap.size());

    // add topic
    TopicBatchCreationRequest topicCreateRequest;
    TopicBatchCreationResponse topicCreateResponse;

    for (int32_t i = 0; i < 3; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        TopicCreationRequest *meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname(topicName);
        meta->set_partitioncount(i + 1);
        if (1 == i) {
            meta->set_topicmode(TOPIC_MODE_SECURITY);
            meta->set_needfieldfilter(true);
            meta->set_obsoletefiletimeinterval(1);
            meta->set_reservedfilecount(2);
            meta->set_deletetopicdata(true);
            meta->set_partitionminbuffersize(32);
            meta->set_partitionmaxbuffersize(64);
            meta->set_topicexpiredtime(11);
            meta->set_dfsroot("");
            meta->add_owners("Jack");
        } else if (2 == i) {
            meta->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
            meta->set_dfsroot("");
            meta->add_owners("Jack");
            meta->add_owners("Smith");
        } else {
            meta->set_dfsroot(_adminConfig.dfsRoot + "/user");
        }
    }
    _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());

    EXPECT_EQ(3, dataManager->_topicAclDataMap.size());

    // delete topic
    TopicBatchDeletionRequest delRequest;
    TopicBatchDeletionResponse delResponse;
    delRequest.add_topicnames("topic_0");
    delRequest.add_topicnames("topic_1");
    delRequest.add_topicnames("topic_2");
    _sysController->deleteTopicBatch(&delRequest, &delResponse);
    ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
    EXPECT_EQ(0, dataManager->_topicAclDataMap.size());

    dataManager.reset();
    topicAclModule.reset();
    _sysController->_moduleManager->unLoadModule<TopicAclManageModule>();
}

TEST_F(SysControllerTest, testCreateTopicBatchFail) {
    {
        TopicBatchCreationRequest topicCreateRequest;
        TopicBatchCreationResponse topicCreateResponse;
        _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_NOT_LEADER, topicCreateResponse.errorinfo().errcode());
    }
    {
        TopicBatchCreationRequest topicCreateRequest;
        TopicBatchCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
    }
    {
        TopicBatchCreationRequest topicCreateRequest;
        TopicBatchCreationResponse topicCreateResponse;
        // one param fail will result in all fail
        for (int32_t i = 0; i < 3; ++i) {
            string topicName = "topic_" + StringUtil::toString(i);
            TopicCreationRequest *meta = topicCreateRequest.add_topicrequests();
            meta->set_topicname(topicName);
            if (1 == i) {
                meta->set_partitioncount(i + 1);
                meta->set_topicmode(TOPIC_MODE_SECURITY);
                meta->set_needfieldfilter(true);
                meta->set_obsoletefiletimeinterval(1);
                meta->set_reservedfilecount(2);
                meta->set_deletetopicdata(true);
                meta->set_partitionminbuffersize(32);
                meta->set_partitionmaxbuffersize(64);
                meta->set_topicexpiredtime(11);
                meta->set_dfsroot("");
                meta->add_owners("Jack");
            } else if (2 == i) {
                meta->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
                meta->set_dfsroot("");
                meta->add_owners("Jack");
                meta->add_owners("Smith");
                meta->set_partitioncount(i + 1);
            } else { // has no partitions, fail
                meta->set_dfsroot(_adminConfig.dfsRoot + "/user");
            }
        }

        _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
        for (int32_t i = 0; i < 3; ++i) {
            string topicName = "topic_" + StringUtil::toString(i);
            TopicCreationRequest meta;
            ASSERT_FALSE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
            ASSERT_EQ(TopicInfoPtr(), _sysController->_topicTable.findTopic(topicName));
        }
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
    }
    { // same topic should fail
        // SWIFT_ROOT_LOG_SETLEVEL(INFO);
        TopicBatchCreationRequest topicCreateRequest;
        TopicBatchCreationResponse topicCreateResponse;
        TopicCreationRequest *meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname("topic1");
        meta->set_partitioncount(1);
        meta->set_needfieldfilter(true);

        meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname("topic2");
        meta->set_partitioncount(2);

        meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname("topic1");
        meta->set_partitioncount(3);
        meta->set_needschema(true);
        _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
        ASSERT_EQ("ERROR_ADMIN_INVALID_PARAMETER[topic name[topic1] repeated]",
                  topicCreateResponse.errorinfo().errmsg());
        TopicCreationRequest retMeta;
        ASSERT_FALSE(_sysController->_zkDataAccessor->getTopicMeta("topic1", retMeta));
        ASSERT_EQ(TopicInfoPtr(), _sysController->_topicTable.findTopic("topic1"));
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
    }
    { // cleaning topic should fail
        _sysController->_moduleManager->loadModule<CleanDataModule>();
        TopicBatchCreationRequest topicCreateRequest;
        TopicBatchCreationResponse topicCreateResponse;
        TopicCreationRequest *meta = topicCreateRequest.add_topicrequests();
        meta->set_topicname("cad_topic1");
        meta->set_partitioncount(1);
        meta->set_needfieldfilter(true);
        auto cleanDataModule = _sysController->_moduleManager->getModule<CleanDataModule>();
        cleanDataModule->_cleanAtDeleteManager->_cleaningTopics.insert("cad_topic1");
        _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicCreateResponse.errorinfo().errcode());
        ASSERT_EQ("ERROR_ADMIN_INVALID_PARAMETER[topic [cad_topic1] is cleaning]",
                  topicCreateResponse.errorinfo().errmsg());
        _sysController->_moduleManager->unLoadModule<CleanDataModule>();
    }
}

TEST_F(SysControllerTest, testDeleteTopic) {
    _sysController->setPrimary(true);
    string dataRoot = GET_TEMPLATE_DATA_PATH();
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_topicname("topic_0");
    topicCreateRequest.set_dfsroot(dataRoot);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    ASSERT_EQ(1, _sysController->_topicTable._topicMap.size());
    topicCreateRequest.set_topicname("topic_1");
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    ASSERT_EQ(2, _sysController->_topicTable._topicMap.size());
    FileSystem::mkDir(dataRoot + "/topic_0");
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_0"));
    FileSystem::mkDir(dataRoot + "/topic_1");
    ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_1"));
    {
        TopicDeletionRequest delRequest;
        TopicDeletionResponse delResponse;
        delRequest.set_topicname("topic_0");
        _sysController->deleteTopic(&delRequest, &delResponse);
        ASSERT_EQ(1, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_0"));

        delRequest.set_topicname("topic_1");
        delRequest.set_deletedata(true);
        _sysController->deleteTopic(&delRequest, &delResponse);
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(EC_FALSE, FileSystem::isExist(dataRoot + "/topic_1"));
    }
    {
        TopicDeletionRequest delRequest;
        TopicDeletionResponse delResponse;
        delRequest.set_topicname("topic_0");
        _sysController->deleteTopic(&delRequest, &delResponse);
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED, delResponse.errorinfo().errcode());
    }
}

TEST_F(SysControllerTest, testDeleteTopicBatch) {
    string dataRoot = GET_TEMPLATE_DATA_PATH();
    _sysController->setPrimary(true);
    for (size_t i = 1; i < 5; i++) {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        topicCreateRequest.set_partitioncount(6);
        string topicName("topic_" + StringUtil::toString(i));
        topicCreateRequest.set_topicname(topicName);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        ASSERT_EQ(i, _sysController->_topicTable._topicMap.size());
        string topicDataPath = dataRoot + "/topic_" + StringUtil::toString(i);
        FileSystem::mkDir(topicDataPath);
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(topicDataPath));
    }
    ASSERT_EQ(4, _sysController->_topicTable._topicMap.size());
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_1");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(3, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_1"));
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_1");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED, delResponse.errorinfo().errcode());
        ASSERT_EQ(3, _sysController->_topicTable._topicMap.size());
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_2");
        delRequest.add_topicnames("topic_3");
        delRequest.set_deletedata(true);
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(1, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(EC_FALSE, FileSystem::isExist(dataRoot + "/topic_2"));
        ASSERT_EQ(EC_FALSE, FileSystem::isExist(dataRoot + "/topic_3"));
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_2");
        delRequest.add_topicnames("topic_3");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED, delResponse.errorinfo().errcode());
        ASSERT_EQ(1, _sysController->_topicTable._topicMap.size());
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_3");
        delRequest.add_topicnames("topic_4");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_1"));
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(dataRoot + "/topic_4"));
    }
    {
        _sysController->_moduleManager->loadModule<CleanDataModule>();
        auto cleanDataModule = _sysController->_moduleManager->getModule<CleanDataModule>();
        cleanDataModule->_cleanAtDeleteManager->_patterns.push_back("pattern_");
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        topicCreateRequest.set_partitioncount(6);
        string topicName("pattern_topic");
        topicCreateRequest.set_topicname(topicName);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        ASSERT_EQ(1, _sysController->_topicTable._topicMap.size());
        string topicDataPath = dataRoot + "/pattern_topic";
        FileSystem::mkDir(topicDataPath);
        ASSERT_EQ(EC_TRUE, FileSystem::isExist(topicDataPath));
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("pattern_topic");
        ASSERT_EQ(0, cleanDataModule->_cleanAtDeleteManager->_cleanAtDeleteTasks.tasks_size());
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(1, cleanDataModule->_cleanAtDeleteManager->_cleanAtDeleteTasks.tasks_size());
        ASSERT_EQ(0, _sysController->_topicTable._topicMap.size());
        ASSERT_EQ(0, _sysController->_deletedTopicMap.count("pattern_topic"));
        CleanAtDeleteTopicTasks emptyTasks;
        EXPECT_TRUE(_sysController->_zkDataAccessor->serializeCleanAtDeleteTasks(emptyTasks));
        cleanDataModule->_cleanAtDeleteManager->_patterns.clear();
        _sysController->_moduleManager->unLoadModule<CleanDataModule>();
    }
}

TEST_F(SysControllerTest, testDeleteTopicBatch_LP) {
    string dataRoot = GET_TEMPLATE_DATA_PATH();
    _sysController->setPrimary(true);
    TopicBatchCreationRequest createRequest;
    TopicBatchCreationResponse createResponse;
    TopicCreationRequest meta;
    meta.set_partitioncount(2);
    meta.set_topicname("topic_n");
    meta.set_topictype(TOPIC_TYPE_NORMAL);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_p-111-1");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_pl");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    *meta.add_physictopiclst() = "topic_pl-111-1";
    meta.set_sealed(true);
    *createRequest.add_topicrequests() = meta;
    meta.clear_physictopiclst();
    meta.set_topicname("topic_pl-111-1");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    meta.set_sealed(false);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_pl2");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_l");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    *meta.add_physictopiclst() = "topic_l-111-1";
    *meta.add_physictopiclst() = "topic_l-222-2";
    *createRequest.add_topicrequests() = meta;
    meta.clear_physictopiclst();
    meta.set_topicname("topic_l-111-1");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    meta.set_sealed(true);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_l-222-2");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    *createRequest.add_topicrequests() = meta;
    _sysController->createTopicBatch(&createRequest, &createResponse);
    ASSERT_EQ(ERROR_NONE, createResponse.errorinfo().errcode());
    ASSERT_EQ(8, _sysController->_topicTable._topicMap.size());

    auto &topicMap = _sysController->_topicTable._topicMap;
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_notexsit");
        delRequest.add_topicnames("topic_notexsit2");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED, delResponse.errorinfo().errcode());
        ASSERT_EQ(8, topicMap.size());
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_p-111-1");
        delRequest.add_topicnames("topic_l-222-2");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_ADMIN_INVALID_PARAMETER, delResponse.errorinfo().errcode());
        ASSERT_EQ(8, topicMap.size());
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_n");
        delRequest.add_topicnames("topic_p-111-1");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(7, topicMap.size());
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_p-111-1"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_pl"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_pl-111-1"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_pl2"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_l"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_l-111-1"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_l-222-2"));
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_pl");
        delRequest.add_topicnames("topic_l");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(2, topicMap.size());
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_p-111-1"));
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_pl2"));
    }
    {
        TopicBatchDeletionRequest delRequest;
        TopicBatchDeletionResponse delResponse;
        delRequest.add_topicnames("topic_pl2");
        _sysController->deleteTopicBatch(&delRequest, &delResponse);
        ASSERT_EQ(protocol::ERROR_NONE, delResponse.errorinfo().errcode());
        ASSERT_EQ(1, topicMap.size());
        ASSERT_TRUE(topicMap.end() != topicMap.find("topic_p-111-1"));
    }
}

TEST_F(SysControllerTest, testDeleteMap) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    topicCreateRequest.set_partitioncount(6);
    string topicName("topic_delmap");
    topicCreateRequest.set_topicname(topicName);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    ASSERT_EQ(0, _sysController->_deletedTopicMap.size());

    int64_t curTime = TimeUtility::currentTimeInSeconds();
    TopicDeletionRequest delRequest;
    TopicDeletionResponse delResponse;
    delRequest.set_topicname(topicName);
    _sysController->deleteTopic(&delRequest, &delResponse);
    ASSERT_EQ(1, _sysController->_deletedTopicMap.size());
    int64_t delTime = _sysController->_deletedTopicMap[topicName];
    ASSERT_TRUE(curTime <= delTime);
    ASSERT_TRUE(delTime <= curTime + 1);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(0, _sysController->_deletedTopicMap.size());
}

TEST_F(SysControllerTest, testCreateTopicLimit) {
    {
        _adminConfig.workerPartitionLimit = 1;
        _adminConfig.topicResourceLimit = 500;
        _adminConfig.groupBrokerCountMap["default"] = 5;
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName = "abcd";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(5);
        topicCreateRequest.set_partitionlimit(6);
        topicCreateRequest.set_resource(1000);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.resource(), 500);
        ASSERT_EQ(meta.partitionlimit(), 1);
    }
    {
        _adminConfig.workerPartitionLimit = 1;
        _adminConfig.topicResourceLimit = 500;
        _adminConfig.groupBrokerCountMap["default"] = 2;
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName = "abcd12";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(5);
        topicCreateRequest.set_partitionlimit(6);
        topicCreateRequest.set_resource(10);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.resource(), 10);
        ASSERT_EQ(meta.partitionlimit(), 3);
    }
}

TEST_F(SysControllerTest, testCreateTopicUpdataGroupName) {
    _adminConfig.topicGroupVec.emplace_back("aaa", "group1");
    _adminConfig.topicGroupVec.emplace_back("aa", "group2");
    _adminConfig.workerPartitionLimit = 1;
    _adminConfig.groupBrokerCountMap["group1"] = 5;
    _adminConfig.groupBrokerCountMap["group2"] = 3;
    _adminConfig.groupBrokerCountMap["default"] = 2;
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName = "topic_aaa";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(5);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.partitionlimit(), 1);
        ASSERT_EQ("group1", meta.topicgroup());
    }
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName = "topic_aa";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(5);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.partitionlimit(), 2);
        ASSERT_EQ("group2", meta.topicgroup());
    }
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName = "topic_a";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(5);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.partitionlimit(), 3);
        ASSERT_EQ("default", meta.topicgroup());
    }
}

TEST_F(SysControllerTest, testCreateTopicUpdateOwners) {
    vector<string> owners1 = {"yida", "shouj"};
    _adminConfig.topicOwnerVec.emplace_back("bs_process", owners1);
    vector<string> owners2 = {"zhongye"};
    _adminConfig.topicOwnerVec.emplace_back("ha3_bahama", owners2);
    _sysController->setPrimary(true);
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName("topic1");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(1);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(topicCreateRequest.topicname(), meta.topicname());
        ASSERT_EQ(0, meta.owners_size());
    }
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName("ha3_bs_process_mainse_157233235");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(1);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(topicCreateRequest.topicname(), meta.topicname());
        ASSERT_EQ(2, meta.owners_size());
        ASSERT_EQ("yida", meta.owners(0));
        ASSERT_EQ("shouj", meta.owners(1));
    }
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName("ha3_bahamav6_mainse");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(1);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(topicCreateRequest.topicname(), meta.topicname());
        ASSERT_EQ(1, meta.owners_size());
        ASSERT_EQ("zhongye", meta.owners(0));
    }
    {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        _sysController->setPrimary(true);
        string topicName("ha3_bahama_test");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(1);
        topicCreateRequest.add_owners("linson");
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicCreationRequest meta;
        ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta(topicName, meta));
        ASSERT_EQ(topicCreateRequest.topicname(), meta.topicname());
        ASSERT_EQ(1, meta.owners_size());
        ASSERT_EQ("linson", meta.owners(0));
    }
}

TEST_F(SysControllerTest, testLoadTopicInfo) {
    _sysController->setPrimary(true);

    for (int32_t i = 0; i < 2; ++i) {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName = "topic_" + StringUtil::toString(i);
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(6);
        if (1 == i) {
            topicCreateRequest.set_topicmode(TOPIC_MODE_SECURITY);
            topicCreateRequest.set_needfieldfilter(true);
            topicCreateRequest.set_obsoletefiletimeinterval(1);
            topicCreateRequest.set_reservedfilecount(2);
            topicCreateRequest.set_deletetopicdata(true);
            topicCreateRequest.set_partitionminbuffersize(32);
            topicCreateRequest.set_partitionmaxbuffersize(64);
        }
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());

        SysController sysController2(&_adminConfig, nullptr);
        sysController2.init();
        ASSERT_TRUE(sysController2._zkDataAccessor->init(_zkWrapper, _zkRoot));
        ASSERT_TRUE(sysController2.loadTopicInfo());
        TopicInfoPtr topicInfoPtr = sysController2._topicTable.findTopic(topicName);
        ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
        ASSERT_EQ(topicCreateRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
        ASSERT_EQ(topicCreateRequest.resource(), topicInfoPtr->getTopicMeta().resource());
        ASSERT_EQ(topicCreateRequest.partitionlimit(), topicInfoPtr->getTopicMeta().partitionlimit());
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)2, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)32, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize()); // modify buffer
        } else {
            ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            ASSERT_TRUE(!topicInfoPtr->getTopicMeta().needfieldfilter());
            ASSERT_EQ((int64_t)-1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
            ASSERT_EQ((int32_t)-1, topicInfoPtr->getTopicMeta().reservedfilecount());
            ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
            ASSERT_EQ((uint32_t)8, topicInfoPtr->getTopicMeta().partitionminbuffersize());
            ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
        }
    }
    { // partition info file not exist
        fslib::ErrorCode ec;
        ec = fslib::fs::FileSystem::move(_zkRoot + "partition_info", _zkRoot + "partition_info_bak");
        ASSERT_TRUE(ec == EC_OK);
        SysController sysController3(&_adminConfig, nullptr);
        sysController3.init();
        ASSERT_TRUE(sysController3._zkDataAccessor->init(_zkWrapper, _zkRoot));
        ASSERT_TRUE(sysController3.loadTopicInfo());
        for (int32_t i = 0; i < 2; ++i) {
            string topicName = "topic_" + StringUtil::toString(i);
            TopicInfoPtr topicInfoPtr = sysController3._topicTable.findTopic(topicName);
            ASSERT_TRUE(topicInfoPtr);
            ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
            if (1 == i) {
                ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            } else {
                ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            }
        }
    }
    { // partition info file error, admin follower
        fslib::ErrorCode ec;
        ec = fslib::fs::FileSystem::writeFile(_zkRoot + "partition_info", "error partition_info_bak");
        ASSERT_TRUE(ec == EC_OK);
        SysController sysController4(&_adminConfig, nullptr);
        sysController4.init();
        ASSERT_TRUE(sysController4._zkDataAccessor->init(_zkWrapper, _zkRoot));
        ASSERT_TRUE(sysController4.loadTopicInfo(true));
        string content;
        ec = fslib::fs::FileSystem::readFile(_zkRoot + "partition_info", content);
        ASSERT_TRUE(ec == EC_OK);
        ASSERT_EQ("error partition_info_bak", content);
        for (int32_t i = 0; i < 2; ++i) {
            string topicName = "topic_" + StringUtil::toString(i);
            TopicInfoPtr topicInfoPtr = sysController4._topicTable.findTopic(topicName);
            ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
            if (1 == i) {
                ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            } else {
                ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            }
        }
    }
    { // partition info file error, admin leader
        fslib::ErrorCode ec;
        ec = fslib::fs::FileSystem::writeFile(_zkRoot + "partition_info", "error partition_info_bak");
        ASSERT_TRUE(ec == EC_OK);
        SysController sysController5(&_adminConfig, nullptr);
        sysController5.init();
        ASSERT_TRUE(sysController5._zkDataAccessor->init(_zkWrapper, _zkRoot));
        ASSERT_TRUE(sysController5.loadTopicInfo(false));
        string content;
        ec = fslib::fs::FileSystem::readFile(_zkRoot + "partition_info", content);
        ASSERT_TRUE(ec == EC_OK);
        ASSERT_FALSE(string("error partition_info_bak") == content);
        for (int32_t i = 0; i < 2; ++i) {
            string topicName = "topic_" + StringUtil::toString(i);
            TopicInfoPtr topicInfoPtr = sysController5._topicTable.findTopic(topicName);
            ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
            if (1 == i) {
                ASSERT_EQ(TOPIC_MODE_SECURITY, topicInfoPtr->getTopicMeta().topicmode());
            } else {
                ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
            }
        }
    }
}

TEST_F(SysControllerTest, testLoadTopicInfoWithUpdateDfsRoot) {
    _sysController->setPrimary(true);
    for (int32_t i = 0; i < 6; ++i) {
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName = "topic_" + StringUtil::toString(i);
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(3);
        if (0 == i) {
            topicCreateRequest.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        }
        if (1 == i) {
            topicCreateRequest.set_dfsroot(_adminConfig.dfsRoot);
        }
        if (2 == i) {
            topicCreateRequest.set_dfsroot(_adminConfig.dfsRoot + "/user");
            *topicCreateRequest.add_extenddfsroot() = _adminConfig.dfsRoot + "/extend";
        }
        if (3 == i) {
            topicCreateRequest.set_dfsroot(_adminConfig.dfsRoot + "/delete");
            *topicCreateRequest.add_extenddfsroot() = _adminConfig.dfsRoot + "/extend";
        }
        if (4 == i) {
            topicCreateRequest.set_dfsroot(_adminConfig.dfsRoot);
            *topicCreateRequest.add_extenddfsroot() = _adminConfig.dfsRoot + "/delete";
        }
        if (5 == i) {
            *topicCreateRequest.add_extenddfsroot() = _adminConfig.dfsRoot + "/extend";
            *topicCreateRequest.add_extenddfsroot() = _adminConfig.dfsRoot + "/delete";
        }
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    }

    AdminConfig conf;
    conf.zkRoot = _adminConfig.zkRoot;
    conf.dfsRoot = _adminConfig.dfsRoot + "/migrate";
    conf.extendDfsRoot = _adminConfig.dfsRoot;
    conf.todelDfsRoot = _adminConfig.dfsRoot + "/delete";
    SysController sysController2(&conf, nullptr);
    sysController2.init();
    ASSERT_TRUE(sysController2._zkDataAccessor->init(_zkWrapper, _zkRoot));
    ASSERT_TRUE(sysController2.loadTopicInfo());

    for (int32_t i = 0; i < 6; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        TopicInfoPtr topicInfoPtr = sysController2._topicTable.findTopic(topicName);
        TopicCreationRequest meta = topicInfoPtr->getTopicMeta();
        ASSERT_EQ(topicName, meta.topicname());
        if (0 == i) {
            ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, meta.topicmode());
            ASSERT_EQ(string(""), meta.dfsroot());
            ASSERT_EQ(string(""), getExtendDfsRoot(meta));
        }
        if (1 == i) {
            ASSERT_EQ(TOPIC_MODE_NORMAL, meta.topicmode());
            ASSERT_EQ(conf.dfsRoot, meta.dfsroot());
            ASSERT_EQ(_adminConfig.dfsRoot + ";", getExtendDfsRoot(meta));
        }
        if (2 == i) {
            ASSERT_EQ(_adminConfig.dfsRoot + "/user", meta.dfsroot());
            ASSERT_EQ(_adminConfig.dfsRoot + "/extend;", getExtendDfsRoot(meta));
        }
        if (3 == i) {
            ASSERT_EQ(conf.dfsRoot, meta.dfsroot());
            ASSERT_EQ(_adminConfig.dfsRoot + "/extend;", getExtendDfsRoot(meta));
        }
        if (4 == i) {
            ASSERT_EQ(conf.dfsRoot, meta.dfsroot());
            ASSERT_EQ(_adminConfig.dfsRoot + ";", getExtendDfsRoot(meta));
        }
        if (5 == i) {
            ASSERT_EQ(conf.dfsRoot, meta.dfsroot());
            ASSERT_EQ(_adminConfig.dfsRoot + "/extend;" + _adminConfig.dfsRoot + ";", getExtendDfsRoot(meta));
        }
    }
}

TEST_F(SysControllerTest, testLoadTopicInfoFailed) {
    _sysController->setPrimary(true);
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(6);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());

    SysController sysController1(&_adminConfig, nullptr);
    sysController1.init();
    ASSERT_TRUE(sysController1._zkDataAccessor->init(_zkWrapper, _zkRoot));
    ASSERT_TRUE(sysController1.loadTopicInfo());
    TopicMap topicMap;
    sysController1._topicTable.getTopicMap(topicMap);
    ASSERT_EQ(size_t(1), topicMap.size());
    _zkServer->stop();
    ASSERT_FALSE(sysController1.loadTopicInfo());
    topicMap.clear();
    sysController1._topicTable.getTopicMap(topicMap);
    ASSERT_EQ(size_t(1), topicMap.size());
}

TEST_F(SysControllerTest, testCleanDataWhenReleaseResource_8503833) {
    _sysController->setPrimary(true);
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(1);
    string testPath = GET_TEMPLATE_DATA_PATH() + "/topic_1/0/file.1";
    fslib::ErrorCode ec = fslib::fs::FileSystem::writeFile(testPath, "123");
    ASSERT_TRUE(ec == EC_OK);
    sleep(1);
    _sysController->_adminConfig->cleanDataIntervalHour = 0.0001;
    _sysController->_adminConfig->reserveDataHour = 0.000001;
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    {
        ScopedLock lock(_sysController->_lock);
        _sysController->_moduleManager->loadModule<CleanDataModule>();
        _sysController->_topicTable.clear();
        sleep(1);
    }
    sleep(1);
    ec = fslib::fs::FileSystem::isExist(testPath);
    ASSERT_EQ(EC_FALSE, ec);
    _sysController->_moduleManager->unLoadModule<CleanDataModule>();
}

TEST_F(SysControllerTest, testCleanZkData) {
    _sysController->setPrimary(true);
    string lockPath1 = _zkRoot + "/lock/broker/default##broker_2_0";
    string lockPath2 = _zkRoot + "/lock/broker/default##broker_2_9";
    string lockPath3 = _zkRoot + "/lock/broker/default##broker_2_10";
    string lockPath4 = _zkRoot + "/lock/broker/default##broker_2_19";
    string taskPath1 = _zkRoot + "/tasks/default##broker_2_1";
    string taskPath2 = _zkRoot + "/tasks/default##broker_2_0";
    string taskPath3 = _zkRoot + "/tasks/default##broker_2_9";
    string taskPath4 = _zkRoot + "/tasks/default##broker_2_10";
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(lockPath1, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(lockPath2, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(lockPath3, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(lockPath4, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(taskPath1, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(taskPath2, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(taskPath3, "123"));
    ASSERT_TRUE(EC_OK == fslib::fs::FileSystem::writeFile(taskPath4, "123"));
    _sysController->_versionManager.currentBrokerRoleVersion = "9";
    _sysController->_versionManager.targetBrokerRoleVersion = "10";
    sleep(1);
    _sysController->_adminConfig->cleanDataIntervalHour = 100;
    {
        ScopedLock lock(_sysController->_lock);
        _sysController->_moduleManager->loadModule<CleanDataModule>();
        sleep(1);
    }
    sleep(1);
    ASSERT_EQ(EC_FALSE, fslib::fs::FileSystem::isExist(lockPath1));
    ASSERT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(lockPath2));
    ASSERT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(lockPath3));
    ASSERT_EQ(EC_FALSE, fslib::fs::FileSystem::isExist(lockPath4));

    ASSERT_EQ(EC_FALSE, fslib::fs::FileSystem::isExist(taskPath1));
    ASSERT_EQ(EC_FALSE, fslib::fs::FileSystem::isExist(taskPath2));
    ASSERT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(taskPath3));
    ASSERT_EQ(EC_TRUE, fslib::fs::FileSystem::isExist(taskPath4));
    _sysController->_moduleManager->unLoadModule<CleanDataModule>();
}

TEST_F(SysControllerTest, testModifyTopicInfo) {
    _sysController->setPrimary(true);
    string address = "127.0.0.1";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_partitionminbuffersize(1);
    topicCreateRequest.set_partitionminbuffersize(2);
    topicCreateRequest.set_resource(20);
    topicCreateRequest.set_partitionlimit(15);
    topicCreateRequest.set_reservedfilecount(20);
    topicCreateRequest.set_obsoletefiletimeinterval(12);
    topicCreateRequest.set_needfieldfilter(true);
    *topicCreateRequest.add_extenddfsroot() = "hdfs://aaa";
    *topicCreateRequest.add_owners() = "Lilei";
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);
    ASSERT_EQ(-1, topicInfoPtr->getTopicMeta().readsizelimitsec());
    ASSERT_EQ(false, topicInfoPtr->getTopicMeta().enablelongpolling());

    TopicCreationRequest topicModifyRequest;
    TopicCreationResponse topicModifyResponse;
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_partitioncount(11);
    topicModifyRequest.set_resource(21);
    topicModifyRequest.set_partitionlimit(16);
    topicModifyRequest.set_needfieldfilter(false);
    topicModifyRequest.set_partitionminbuffersize(9999);
    topicModifyRequest.set_partitionmaxbuffersize(9999);
    topicModifyRequest.set_topicexpiredtime(1009);
    topicModifyRequest.set_compressmsg(true);
    topicModifyRequest.set_compressthres(100);
    topicModifyRequest.set_dfsroot("hdfs://xxx");
    *topicModifyRequest.add_extenddfsroot() = "hdfs://aaa";
    *topicModifyRequest.add_extenddfsroot() = "hdfs://bbb";
    *topicModifyRequest.add_owners() = "Jack";
    *topicModifyRequest.add_owners() = "Smith";
    topicModifyRequest.set_enablettldel(false);
    topicModifyRequest.set_readsizelimitsec(200);
    topicModifyRequest.set_enablelongpolling(true);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);
    ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
    ASSERT_EQ(topicModifyRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
    ASSERT_EQ(topicModifyRequest.partitionminbuffersize(), topicInfoPtr->getTopicMeta().partitionminbuffersize());
    ASSERT_EQ(topicModifyRequest.partitionmaxbuffersize(), topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
    ASSERT_EQ(topicCreateRequest.reservedfilecount(), topicInfoPtr->getTopicMeta().reservedfilecount());
    ASSERT_EQ(topicCreateRequest.needfieldfilter(), topicInfoPtr->getTopicMeta().needfieldfilter());
    ASSERT_EQ(topicCreateRequest.obsoletefiletimeinterval(), topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
    ASSERT_EQ(topicModifyRequest.resource(), topicInfoPtr->getTopicMeta().resource());
    ASSERT_EQ(topicModifyRequest.partitionlimit(), topicInfoPtr->getTopicMeta().partitionlimit());
    ASSERT_EQ(topicModifyRequest.topicexpiredtime(), topicInfoPtr->getTopicMeta().topicexpiredtime());
    ASSERT_EQ(topicCreateRequest.maxwaittimeforsecuritycommit(),
              topicInfoPtr->getTopicMeta().maxwaittimeforsecuritycommit());
    ASSERT_EQ(topicModifyRequest.compressmsg(), topicInfoPtr->getTopicMeta().compressmsg());
    ASSERT_EQ(topicModifyRequest.compressthres(), topicInfoPtr->getTopicMeta().compressthres());
    ASSERT_EQ(topicModifyRequest.dfsroot(), topicInfoPtr->getTopicMeta().dfsroot());
    ASSERT_EQ(2, topicInfoPtr->getTopicMeta().extenddfsroot_size());
    ASSERT_EQ(string("hdfs://aaa"), topicInfoPtr->getTopicMeta().extenddfsroot(0));
    ASSERT_EQ(string("hdfs://bbb"), topicInfoPtr->getTopicMeta().extenddfsroot(1));
    ASSERT_EQ(2, topicInfoPtr->getTopicMeta().owners_size());
    ASSERT_EQ(string("Jack"), topicInfoPtr->getTopicMeta().owners(0));
    ASSERT_EQ(string("Smith"), topicInfoPtr->getTopicMeta().owners(1));
    ASSERT_FALSE(topicInfoPtr->getTopicMeta().enablettldel());
    ASSERT_EQ(200, topicInfoPtr->getTopicMeta().readsizelimitsec());
    ASSERT_TRUE(topicInfoPtr->getTopicMeta().enablelongpolling());
    ASSERT_FALSE(topicInfoPtr->getTopicMeta().versioncontrol());
    TopicCreationRequest meta;
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    auto modifyTime1 = meta.modifytime();
    ASSERT_FALSE(meta.createtime() == modifyTime1);
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.partitioncount(), topicModifyRequest.partitioncount());
    ASSERT_EQ(meta.partitionminbuffersize(), topicModifyRequest.partitionminbuffersize());
    ASSERT_EQ(meta.partitionmaxbuffersize(), topicModifyRequest.partitionmaxbuffersize());
    ASSERT_EQ(meta.reservedfilecount(), topicCreateRequest.reservedfilecount());
    ASSERT_EQ(meta.needfieldfilter(), topicCreateRequest.needfieldfilter());
    ASSERT_EQ(meta.obsoletefiletimeinterval(), topicCreateRequest.obsoletefiletimeinterval());
    ASSERT_EQ(meta.resource(), topicModifyRequest.resource());
    ASSERT_EQ(meta.partitionlimit(), topicModifyRequest.partitionlimit());
    ASSERT_EQ(meta.topicexpiredtime(), topicModifyRequest.topicexpiredtime());
    ASSERT_EQ(meta.compressmsg(), topicModifyRequest.compressmsg());
    ASSERT_EQ(meta.compressthres(), topicModifyRequest.compressthres());
    ASSERT_EQ(meta.dfsroot(), topicModifyRequest.dfsroot());
    ASSERT_FALSE(meta.enablettldel());
    ASSERT_EQ(2, meta.extenddfsroot_size());
    ASSERT_EQ(string("hdfs://aaa"), meta.extenddfsroot(0));
    ASSERT_EQ(string("hdfs://bbb"), meta.extenddfsroot(1));
    ASSERT_EQ(2, meta.owners_size());
    ASSERT_EQ(string("Jack"), meta.owners(0));
    ASSERT_EQ(string("Smith"), meta.owners(1));
    ASSERT_FALSE(meta.has_readnotcommittedmsg());
    ASSERT_TRUE(meta.readnotcommittedmsg());

    // verify version change
    *topicModifyRequest.add_owners() = "Linson";
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_EQ(3, topicInfoPtr->getTopicMeta().owners_size());
    ASSERT_EQ(string("Jack"), topicInfoPtr->getTopicMeta().owners(0));
    ASSERT_EQ(string("Smith"), topicInfoPtr->getTopicMeta().owners(1));
    ASSERT_EQ(string("Linson"), topicInfoPtr->getTopicMeta().owners(2));
    auto modifyTime2 = meta.modifytime();
    EXPECT_EQ(modifyTime1, modifyTime2); // not change
    ASSERT_EQ(3, meta.owners_size());
    ASSERT_EQ(string("Jack"), meta.owners(0));
    ASSERT_EQ(string("Smith"), meta.owners(1));
    ASSERT_EQ(string("Linson"), meta.owners(2));

    topicModifyRequest.clear_owners();
    *topicModifyRequest.add_owners() = "Jack";
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_EQ(1, topicInfoPtr->getTopicMeta().owners_size());
    ASSERT_EQ(string("Jack"), topicInfoPtr->getTopicMeta().owners(0));
    auto modifyTime3 = meta.modifytime();
    EXPECT_EQ(modifyTime3, modifyTime2); // not change
    ASSERT_EQ(1, meta.owners_size());
    ASSERT_EQ(string("Jack"), meta.owners(0));

    topicModifyRequest.set_readnotcommittedmsg(false);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_FALSE(topicInfoPtr->readNotCommittedMsg());
    auto modifyTime4 = meta.modifytime();
    EXPECT_GE(modifyTime4, modifyTime3); // change
    ASSERT_FALSE(meta.readnotcommittedmsg());

    topicModifyRequest.set_readnotcommittedmsg(true);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr->readNotCommittedMsg());
    auto modifyTime5 = meta.modifytime();
    EXPECT_GE(modifyTime5, modifyTime4); // change
    ASSERT_TRUE(meta.readnotcommittedmsg());
}

TEST_F(SysControllerTest, testModifyTopic_LP) {
    _sysController->setPrimary(true);
    string address = "127.0.0.1";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_topictype(TOPIC_TYPE_NORMAL);
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_partitionlimit(15);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);

    // modify success
    TopicCreationRequest topicModifyRequest;
    TopicCreationResponse topicModifyResponse;
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    topicModifyRequest.set_partitioncount(6);
    topicModifyRequest.set_partitionlimit(20);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);
    ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
    ASSERT_EQ(topicModifyRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
    ASSERT_EQ(topicModifyRequest.partitionlimit(), topicInfoPtr->getTopicMeta().partitionlimit());
    TopicCreationRequest meta;
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    ASSERT_EQ(meta.topicname(), topicModifyRequest.topicname());
    ASSERT_EQ(meta.partitioncount(), topicModifyRequest.partitioncount());
    ASSERT_EQ(meta.partitionlimit(), topicModifyRequest.partitionlimit());

    // modify fail
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_topictype(TOPIC_TYPE_LOGIC);
    topicModifyRequest.set_partitioncount(6);
    topicModifyRequest.set_partitionlimit(25);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, topicModifyResponse.errorinfo().errcode());
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);
    ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
    ASSERT_EQ(6, topicInfoPtr->getTopicMeta().partitioncount());
    ASSERT_EQ(20, topicInfoPtr->getTopicMeta().partitionlimit());

    // modify partition count
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    topicModifyRequest.set_partitioncount(8);
    topicModifyRequest.set_partitionlimit(20);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    auto &tasks = _sysController->_zkDataAccessor->_changePartCntTasks;
    ASSERT_EQ(1, tasks.tasks_size());
    int64_t taskId = tasks.tasks(0).taskid();
    ASSERT_EQ(topicModifyRequest.topicname(), tasks.tasks(0).meta().topicname());

    // modify partition count(same task)
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    topicModifyRequest.set_partitioncount(3);
    topicModifyRequest.set_partitionlimit(20);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);
    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    tasks = _sysController->_zkDataAccessor->_changePartCntTasks;
    ASSERT_EQ(1, tasks.tasks_size());
    ASSERT_EQ(taskId, tasks.tasks(0).taskid());
    ASSERT_EQ(topicModifyRequest.topicname(), tasks.tasks(0).meta().topicname());
    ASSERT_EQ(8, tasks.tasks(0).meta().partitioncount());

    // zk meta not change
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    ASSERT_EQ(meta.topicname(), topicModifyRequest.topicname());
    ASSERT_EQ(TOPIC_TYPE_LOGIC_PHYSIC, meta.topictype());
    ASSERT_EQ(6, meta.partitioncount());
    ASSERT_EQ(20, meta.partitionlimit());
    ASSERT_EQ(false, meta.sealed());
}

TEST_F(SysControllerTest, testModifyTopic_CanNotModifySuccess) {
    _sysController->setPrimary(true);
    string address = "127.0.0.1";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_dfsroot("hdfs://xxx");
    *topicCreateRequest.add_extenddfsroot() = "hdfs://aaa";
    *topicCreateRequest.add_owners() = "Lilei";
    topicCreateRequest.set_sealed(true);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);

    TopicCreationRequest topicModifyRequest;
    TopicCreationResponse topicModifyResponse;
    topicModifyRequest.set_topicname(topicName);
    topicModifyRequest.set_partitioncount(6);
    topicModifyRequest.set_dfsroot("hdfs://xxx");
    *topicModifyRequest.add_extenddfsroot() = "hdfs://aaa";
    *topicModifyRequest.add_owners() = "Jack";
    *topicModifyRequest.add_owners() = "Smith";
    topicModifyRequest.set_sealed(true);
    _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);

    ASSERT_EQ(ERROR_NONE, topicModifyResponse.errorinfo().errcode());
    topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);

    TopicCreationRequest meta;
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.partitioncount(), topicModifyRequest.partitioncount());
    ASSERT_EQ(meta.dfsroot(), topicModifyRequest.dfsroot());
    ASSERT_EQ(1, meta.extenddfsroot_size());
    ASSERT_EQ(string("hdfs://aaa"), meta.extenddfsroot(0));
    ASSERT_EQ(2, meta.owners_size());
    ASSERT_EQ(string("Jack"), meta.owners(0));
    ASSERT_EQ(string("Smith"), meta.owners(1));
}

TEST_F(SysControllerTest, testModifyTopic_CanNotModifyFailed) {
    { // version_control cannot modify
        _sysController->setPrimary(true);
        TopicBatchCreationRequest batchRequest;
        TopicBatchCreationResponse batchResponse;
        TopicCreationRequest crequest;
        crequest.set_topicname("topic_vc");
        crequest.set_versioncontrol(true);
        crequest.set_partitioncount(1);
        *batchRequest.add_topicrequests() = crequest;
        _sysController->createTopicBatch(&batchRequest, &batchResponse);
        ASSERT_EQ(ERROR_NONE, batchResponse.errorinfo().errcode());

        TopicCreationRequest mrequest;
        TopicCreationResponse mresponse;
        mrequest.set_topicname("topic_vc");
        mrequest.set_versioncontrol(false);
        _sysController->modifyTopic(&mrequest, &mresponse);
        ASSERT_EQ(ERROR_NONE, mresponse.errorinfo().errcode());
        TopicCreationRequest meta;
        _sysController->_zkDataAccessor->getTopicMeta("topic_vc", meta);
        ASSERT_EQ("topic_vc", meta.topicname());
        ASSERT_FALSE(meta.versioncontrol());

        // other param can modify
        mrequest.set_topicname("topic_vc");
        mrequest.set_versioncontrol(true);
        _sysController->modifyTopic(&mrequest, &mresponse);
        ASSERT_EQ(ERROR_NONE, mresponse.errorinfo().errcode());
    }
    { // sealed topic cannot modify
        _sysController->setPrimary(true);
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName = "topic_0";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(6);
        topicCreateRequest.set_sealed(true);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);

        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
        ASSERT_TRUE(topicInfoPtr);

        TopicCreationRequest topicModifyRequest;
        TopicCreationResponse topicModifyResponse;
        topicModifyRequest.set_topicname(topicName);
        topicModifyRequest.set_partitioncount(6);
        topicModifyRequest.set_sealed(false);
        _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);

        ASSERT_EQ(ERROR_ADMIN_SEALED_TOPIC_CANNOT_MODIFY, topicModifyResponse.errorinfo().errcode());

        TopicCreationRequest meta;
        _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
        ASSERT_EQ("topic_0", meta.topicname());
        ASSERT_EQ(6, meta.partitioncount());
        ASSERT_TRUE(meta.sealed());
    }
    {
        _sysController->setPrimary(true);
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName = "topic_1";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.add_owners("Jack");
        topicCreateRequest.set_partitioncount(6);
        *topicCreateRequest.add_extenddfsroot() = "hdfs://aaa";
        topicCreateRequest.set_sealed(true);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);

        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
        ASSERT_TRUE(topicInfoPtr);

        TopicCreationRequest topicModifyRequest;
        TopicCreationResponse topicModifyResponse;
        topicModifyRequest.set_topicname(topicName);
        topicModifyRequest.add_owners("Bob");
        topicModifyRequest.set_partitioncount(5);
        *topicModifyRequest.add_extenddfsroot() = "hdfs://aaa";
        *topicModifyRequest.add_extenddfsroot() = "hdfs://bbb";
        topicModifyRequest.set_sealed(true);
        _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);

        ASSERT_EQ(ERROR_ADMIN_SEALED_TOPIC_CANNOT_MODIFY, topicModifyResponse.errorinfo().errcode());

        TopicCreationRequest meta;
        _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.owners_size(), topicCreateRequest.owners_size());
        ASSERT_EQ(meta.owners(0), topicCreateRequest.owners(0));
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.extenddfsroot_size(), topicCreateRequest.extenddfsroot_size());
        ASSERT_EQ(meta.extenddfsroot(0), topicCreateRequest.extenddfsroot(0));
        ASSERT_EQ(meta.sealed(), topicCreateRequest.sealed());
    }
    {
        _sysController->setPrimary(true);
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName = "topic_3";
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.add_owners("Jack");
        topicCreateRequest.set_partitioncount(6);
        *topicCreateRequest.add_extenddfsroot() = "hdfs://aaa";
        topicCreateRequest.set_sealed(true);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);

        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
        ASSERT_TRUE(topicInfoPtr);

        TopicCreationRequest topicModifyRequest;
        TopicCreationResponse topicModifyResponse;
        topicModifyRequest.set_topicname("topic_3");
        topicModifyRequest.add_owners("Jack");
        topicModifyRequest.set_partitioncount(3);
        *topicModifyRequest.add_extenddfsroot() = "hdfs://aaa";
        topicModifyRequest.set_sealed(true);
        _sysController->modifyTopic(&topicModifyRequest, &topicModifyResponse);

        ASSERT_EQ(ERROR_ADMIN_SEALED_TOPIC_CANNOT_MODIFY, topicModifyResponse.errorinfo().errcode());

        TopicCreationRequest meta;
        _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
        ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
        ASSERT_EQ(meta.owners_size(), topicCreateRequest.owners_size());
        ASSERT_EQ(meta.owners(0), topicCreateRequest.owners(0));
        ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());
        ASSERT_EQ(meta.sealed(), topicCreateRequest.sealed());
    }
    { // cannot seal logic topic
        _sysController->setPrimary(true);
        TopicCreationRequest logicRequest, lpRequest;
        TopicCreationResponse response;
        logicRequest.set_topictype(TOPIC_TYPE_LOGIC);
        logicRequest.set_topicname("logic_topic");
        logicRequest.set_partitioncount(2);
        logicRequest.set_sealed(false);
        *logicRequest.add_physictopiclst() = "logic_topic-100-2";
        _sysController->createTopic(&logicRequest, &response);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
        lpRequest.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        lpRequest.set_topicname("logic_physic_topic");
        lpRequest.set_partitioncount(1);
        lpRequest.set_sealed(false);
        _sysController->createTopic(&lpRequest, &response);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());

        ASSERT_TRUE(_sysController->_topicTable.findTopic("logic_topic"));
        ASSERT_TRUE(_sysController->_topicTable.findTopic("logic_topic-100-2"));
        ASSERT_TRUE(_sysController->_topicTable.findTopic("logic_physic_topic"));
        auto topicMap = _sysController->_topicTable._topicMap;

        TopicCreationRequest modifyRequest = logicRequest;
        modifyRequest.set_sealed(true);
        _sysController->modifyTopic(&modifyRequest, &response);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
        modifyRequest = lpRequest;
        modifyRequest.set_sealed(true);
        _sysController->modifyTopic(&modifyRequest, &response);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

        TopicCreationRequest meta;
        _sysController->_zkDataAccessor->getTopicMeta("logic_topic", meta);
        ASSERT_EQ("logic_topic", meta.topicname());
        ASSERT_EQ(2, meta.partitioncount());
        ASSERT_FALSE(meta.sealed());
        _sysController->_zkDataAccessor->getTopicMeta("logic_physic_topic", meta);
        ASSERT_EQ("logic_physic_topic", meta.topicname());
        ASSERT_EQ(1, meta.partitioncount());
        ASSERT_FALSE(meta.sealed());
    }
}

TEST_F(SysControllerTest, testModifyFailed) {
    _sysController->setPrimary(true);
    string address = "127.0.0.1";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_partitionminbuffersize(1);
    topicCreateRequest.set_partitionminbuffersize(2);
    topicCreateRequest.set_resource(20);
    topicCreateRequest.set_partitionlimit(15);
    topicCreateRequest.set_reservedfilecount(20);
    topicCreateRequest.set_obsoletefiletimeinterval(12);
    topicCreateRequest.set_needfieldfilter(true);
    *topicCreateRequest.add_extenddfsroot() = "hdfs://aaa";
    *topicCreateRequest.add_owners() = "Lilei";
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_TRUE(topicInfoPtr);
    ASSERT_EQ(-1, topicInfoPtr->getTopicMeta().readsizelimitsec());
    ASSERT_EQ(false, topicInfoPtr->getTopicMeta().enablelongpolling());
}

TEST_F(SysControllerTest, testUpdateWorkerStatus) {
    WorkerMap workerMap;
    MockSysController mockSysController(nullptr, nullptr);
    mockSysController.setPrimary(false);
    EXPECT_CALL(mockSysController, updateBrokerWorkerStatusForEmptyTarget(_)).Times(0);
    mockSysController.updateBrokerWorkerStatus(workerMap);

    _sysController->setPrimary(true);
    string address = "role_1#_#127.0.0.1:1111";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName1 = "topic_1";
    topicCreateRequest.set_topicname(topicName1);
    topicCreateRequest.set_partitioncount(6);
    topicCreateRequest.set_partitionminbuffersize(10);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr1 = _sysController->_topicTable.findTopic(topicName1);
    ASSERT_TRUE(topicInfoPtr1);
    topicInfoPtr1->setCurrentRoleAddress(0, address);

    string topicName2 = "topic_2";
    topicCreateRequest.set_partitionminbuffersize(20);
    topicCreateRequest.set_topicname(topicName2);
    topicCreateRequest.set_topicmode(TOPIC_MODE_SECURITY);
    topicCreateRequest.set_needfieldfilter(true);
    topicCreateRequest.set_obsoletefiletimeinterval(1);
    topicCreateRequest.set_reservedfilecount(2);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr2 = _sysController->_topicTable.findTopic(topicName2);
    ASSERT_TRUE(topicInfoPtr2);
    topicInfoPtr2->setCurrentRoleAddress(1, address);

    RoleInfo roleInfo("role_1", "127.0.0.1", 1111);
    WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
    HeartbeatInfo current;
    current.set_address(address);
    workerInfoPtr->setCurrent(current);
    workerInfoPtr->addTask2target(topicName1, 0, 30, 100000);
    workerInfoPtr->addTask2target(topicName2, 1, 30, 100000);
    workerMap["role_1"] = workerInfoPtr;
    _sysController->updateBrokerWorkerStatus(workerMap);

    DispatchInfo dispatchInfo;
    _sysController->_zkDataAccessor->getDispatchedTask("role_1", dispatchInfo);
    ASSERT_EQ("127.0.0.1:1111", dispatchInfo.brokeraddress());
    ASSERT_EQ("role_1", dispatchInfo.rolename());
    ASSERT_EQ(2, dispatchInfo.taskinfos_size());

    const TaskInfo &taskInfo1 = dispatchInfo.taskinfos(0);
    ASSERT_EQ(TOPIC_MODE_NORMAL, taskInfo1.topicmode());
    ASSERT_EQ((uint32_t)10, taskInfo1.minbuffersize());
    ASSERT_EQ((uint32_t)0, taskInfo1.partitionid().id());
    ASSERT_EQ(topicName1, taskInfo1.partitionid().topicname());
    ASSERT_EQ((uint32_t)1, taskInfo1.partitionid().rangecount());
    ASSERT_EQ((uint32_t)0, taskInfo1.partitionid().from());
    ASSERT_EQ((uint32_t)10922, taskInfo1.partitionid().to());
    ASSERT_TRUE(!taskInfo1.needfieldfilter());
    ASSERT_EQ((int64_t)-1, taskInfo1.obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)-1, taskInfo1.reservedfilecount());

    const TaskInfo &taskInfo2 = dispatchInfo.taskinfos(1);
    ASSERT_EQ(TOPIC_MODE_SECURITY, taskInfo2.topicmode());
    ASSERT_EQ((uint32_t)20, taskInfo2.minbuffersize());
    ASSERT_EQ((uint32_t)1, taskInfo2.partitionid().id());
    ASSERT_EQ(topicName2, taskInfo2.partitionid().topicname());
    ASSERT_TRUE(taskInfo2.needfieldfilter());
    ASSERT_EQ((int64_t)1, taskInfo2.obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)2, taskInfo2.reservedfilecount());
}

TEST_F(SysControllerTest, testUpdateWorkerStatusStillLoad) {
    _sysController->setPrimary(true);
    string address1 = "role_1#_#127.0.0.1:1111";
    string address2 = "role_2#_#127.0.0.1:2222";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName1 = "topic_1";
    topicCreateRequest.set_topicname(topicName1);
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_partitionminbuffersize(10);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr1 = _sysController->_topicTable.findTopic(topicName1);
    ASSERT_TRUE(topicInfoPtr1);
    topicInfoPtr1->setCurrentRoleAddress(0, address2);

    WorkerMap workerMap;
    RoleInfo roleInfo1("role_1", "127.0.0.1", 1111);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo1));
    HeartbeatInfo current1;
    int64_t curTime = TimeUtility::currentTime();
    TaskStatus *taskStatus = current1.add_tasks();
    taskStatus->mutable_taskinfo()->mutable_partitionid()->set_topicname(topicName1);
    taskStatus->mutable_taskinfo()->mutable_partitionid()->set_id(0);
    current1.set_address(address1);
    workerInfoPtr1->setCurrent(current1);
    workerInfoPtr1->prepareDecision(curTime, -1);
    workerMap["role_1"] = workerInfoPtr1;

    RoleInfo roleInfo2("role_2", "127.0.0.1", 2222);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo2));
    HeartbeatInfo current2;
    current2.set_address(address2);
    workerInfoPtr2->setCurrent(current2);
    workerInfoPtr2->prepareDecision(curTime, -1);
    workerMap["role_2"] = workerInfoPtr2;

    workerInfoPtr2->addTask2target(topicName1, 0, 30, 100000);
    _sysController->updateBrokerWorkerStatus(workerMap);

    DispatchInfo dispatchInfo1;
    _sysController->_zkDataAccessor->getDispatchedTask("role_1", dispatchInfo1);
    ASSERT_EQ("127.0.0.1:1111", dispatchInfo1.brokeraddress());
    ASSERT_EQ("role_1", dispatchInfo1.rolename());
    ASSERT_EQ(0, dispatchInfo1.taskinfos_size());

    DispatchInfo dispatchInfo2;
    _sysController->_zkDataAccessor->getDispatchedTask("role_2", dispatchInfo2);
    ASSERT_EQ("127.0.0.1:2222", dispatchInfo2.brokeraddress());
    ASSERT_EQ("role_2", dispatchInfo2.rolename());
    ASSERT_EQ(0, dispatchInfo2.taskinfos_size());
}

TEST_F(SysControllerTest, testUpdateWorkerStatusForceSchedule) {
    _sysController->setPrimary(true);
    _sysController->_adminConfig->forceScheduleTimeSecond = 10;
    string address1 = "role_1#_#127.0.0.1:1111";
    string address2 = "role_2#_#127.0.0.1:2222";

    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    string topicName1 = "topic_1";
    topicCreateRequest.set_topicname(topicName1);
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_partitionminbuffersize(10);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicInfoPtr topicInfoPtr1 = _sysController->_topicTable.findTopic(topicName1);
    ASSERT_TRUE(topicInfoPtr1);
    topicInfoPtr1->setCurrentRoleAddress(0, address2);

    WorkerMap workerMap;
    RoleInfo roleInfo1("role_1", "127.0.0.1", 1111);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo1));
    HeartbeatInfo current1;
    int64_t curTime = TimeUtility::currentTime();
    TaskStatus *taskStatus = current1.add_tasks();
    taskStatus->mutable_taskinfo()->mutable_partitionid()->set_topicname(topicName1);
    taskStatus->mutable_taskinfo()->mutable_partitionid()->set_id(0);
    current1.set_address(address1);
    workerInfoPtr1->setCurrent(current1);
    workerInfoPtr1->prepareDecision(curTime, -1);
    workerInfoPtr1->_unknownStartTime = curTime - 20 * 1000000;
    workerMap["role_1"] = workerInfoPtr1;

    RoleInfo roleInfo2("role_2", "127.0.0.1", 2222);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo2));
    HeartbeatInfo current2;
    current2.set_address(address2);
    workerInfoPtr2->setCurrent(current2);
    workerInfoPtr2->prepareDecision(curTime, -1);
    workerMap["role_2"] = workerInfoPtr2;

    workerInfoPtr2->addTask2target(topicName1, 0, 30, 100000);
    _sysController->updateBrokerWorkerStatus(workerMap);

    DispatchInfo dispatchInfo1;
    _sysController->_zkDataAccessor->getDispatchedTask("role_1", dispatchInfo1);
    ASSERT_EQ("127.0.0.1:1111", dispatchInfo1.brokeraddress());
    ASSERT_EQ("role_1", dispatchInfo1.rolename());
    ASSERT_EQ(0, dispatchInfo1.taskinfos_size());

    DispatchInfo dispatchInfo2;
    _sysController->_zkDataAccessor->getDispatchedTask("role_2", dispatchInfo2);
    ASSERT_EQ("127.0.0.1:2222", dispatchInfo2.brokeraddress());
    ASSERT_EQ("role_2", dispatchInfo2.rolename());
    ASSERT_EQ(1, dispatchInfo2.taskinfos_size());
}

TEST_F(SysControllerTest, testSetGetSysStatus) {
    EXPECT_FALSE(_sysController->isPrimary());
    _sysController->setPrimary(true);
    EXPECT_TRUE(_sysController->isPrimary());
}

TEST_F(SysControllerTest, testClearWorkerTask) {
    /*
        vector<string> ipVec;
        ipVec.push_back("1.0.0.1");
        ipVec.push_back("1.0.0.2");
        ipVec.push_back("1.0.0.3");
        string topicName = "topic";

        //prepare dispatch task
        for (size_t i = 0; i < ipVec.size(); ++i) {
            DispatchInfo di;
            di.set_brokeraddress(common::PathDefine::getAddress(
                            ipVec[i], config.brokerPort));
            TaskInfo *ti = di.add_taskinfos();
            PartitionId *partId = ti->mutable_partitionid();
            partId->set_topicname(topicName);
            partId->set_id(i);
            ASSERT_EQ(int(0), dataAccessor.setDispatchedTask(di));
        }
        vector<protocol::DispatchInfo> dispatchInfoList;
        dataAccessor.getDispatchInfo(dispatchInfoList);
        ASSERT_EQ(size_t(3), dispatchInfoList.size());
        for (size_t i = 0; i < dispatchInfoList.size(); ++i) {
            ASSERT_EQ(int(1), dispatchInfoList[i].taskinfos_size());
        }

        MachineControlResponse response;
        response.add_deletedlist("1.0.0.1");
        _sysController->syncMachine(ROLE_TYPE_BROKER, &response);
        dispatchInfoList.clear();
        dataAccessor.getDispatchInfo(dispatchInfoList);
        ASSERT_EQ(size_t(3), dispatchInfoList.size());
        for (size_t i = 0; i < dispatchInfoList.size(); ++i) {
            string address = dispatchInfoList[i].brokeraddress();
            if (common::PathDefine::getAddress("1.0.0.1", config.brokerPort)
                == address)
            {
                ASSERT_EQ(int(0), dispatchInfoList[i].taskinfos_size());
            } else {
                ASSERT_EQ(int(1), dispatchInfoList[i].taskinfos_size());
            }
        }

        response.Clear();
        response.add_deletedlist("1.0.0.2");
        response.add_deletedlist("1.0.0.3");
        _sysController->syncMachine(ROLE_TYPE_BROKER, &response);
        dispatchInfoList.clear();
        dataAccessor.getDispatchInfo(dispatchInfoList);
        ASSERT_EQ(size_t(3), dispatchInfoList.size());
        for (size_t i = 0; i < dispatchInfoList.size(); ++i) {
            ASSERT_EQ(int(0), dispatchInfoList[i].taskinfos_size());
        }

        _sysController->syncMachine(ROLE_TYPE_ADMIN, &response);
        dispatchInfoList.clear();
        dataAccessor.getDispatchInfo(dispatchInfoList);
        ASSERT_EQ(size_t(3), dispatchInfoList.size());*/
}

TEST_F(SysControllerTest, testGetAllWorkerStatus) {}

TEST_F(SysControllerTest, testGetAllTopicInfo) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    protocol::EmptyRequest request;
    protocol::AllTopicInfoResponse response;
    protocol::AllTopicSimpleInfoResponse simpleResponse;
    _sysController->getAllTopicInfo(&request, &response);
    _sysController->getAllTopicSimpleInfo(&request, &simpleResponse);
    ASSERT_EQ((int)0, response.alltopicinfo_size());
    ASSERT_EQ((int)0, simpleResponse.alltopicsimpleinfo_size());
    ASSERT_EQ(protocol::ERROR_NONE, response.errorinfo().errcode());
    topicCreateRequest.set_partitioncount(3);
    topicCreateRequest.set_partitionminbuffersize(10);
    topicCreateRequest.set_partitionmaxbuffersize(20);
    for (int32_t i = 0; i < 2; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        topicCreateRequest.set_topicname(topicName);
        if (i == 1) {
            topicCreateRequest.add_owners("Jack");
            topicCreateRequest.add_owners("Smith");
        }
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    }
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic("topic_0");
    assert(topicInfoPtr);
    topicInfoPtr->setCurrentRoleAddress(0, "role_1#_#0.0.0.0");
    topicInfoPtr->setCurrentRoleAddress(1, "role_2#_#0.0.0.1");
    topicInfoPtr->setCurrentRoleAddress(2, "role_3#_#0.0.0.2");
    topicInfoPtr->setStatus(0, protocol::PARTITION_STATUS_WAITING);
    topicInfoPtr->setStatus(1, protocol::PARTITION_STATUS_STARTING);
    topicInfoPtr->setStatus(2, protocol::PARTITION_STATUS_RUNNING);

    topicInfoPtr = _sysController->_topicTable.findTopic("topic_1");
    assert(topicInfoPtr);
    topicInfoPtr->setStatus(0, protocol::PARTITION_STATUS_RUNNING);

    response.Clear();
    _sysController->getAllTopicInfo(&request, &response);
    ASSERT_EQ(protocol::ERROR_NONE, response.errorinfo().errcode());
    ASSERT_EQ((int)2, response.alltopicinfo_size());
    protocol::TopicInfo topicInfo = response.alltopicinfo(0);
    ASSERT_EQ(string("topic_0"), topicInfo.name());
    ASSERT_EQ(protocol::TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.status());
    ASSERT_EQ((uint32_t)3, topicInfo.partitioncount());
    ASSERT_EQ((uint32_t)10, topicInfo.partitionminbuffersize());
    ASSERT_EQ((uint32_t)256, topicInfo.partitionmaxbuffersize());
    ASSERT_EQ((int)3, topicInfo.partitioninfos_size());
    ASSERT_EQ((string) "0.0.0.0", topicInfo.partitioninfos(0).brokeraddress());
    ASSERT_EQ((string) "role_1", topicInfo.partitioninfos(0).rolename());
    ASSERT_EQ(protocol::PARTITION_STATUS_WAITING, topicInfo.partitioninfos(0).status());
    ASSERT_EQ((string) "0.0.0.1", topicInfo.partitioninfos(1).brokeraddress());
    ASSERT_EQ((string) "role_2", topicInfo.partitioninfos(1).rolename());
    ASSERT_EQ(protocol::PARTITION_STATUS_STARTING, topicInfo.partitioninfos(1).status());
    ASSERT_EQ((string) "0.0.0.2", topicInfo.partitioninfos(2).brokeraddress());
    ASSERT_EQ((string) "role_3", topicInfo.partitioninfos(2).rolename());
    ASSERT_EQ(protocol::PARTITION_STATUS_RUNNING, topicInfo.partitioninfos(2).status());

    topicInfo = response.alltopicinfo(1);
    ASSERT_EQ(string("topic_1"), topicInfo.name());
    ASSERT_EQ(protocol::TOPIC_STATUS_PARTIAL_RUNNING, topicInfo.status());
    ASSERT_EQ((uint32_t)3, topicInfo.partitioncount());
    ASSERT_EQ((uint32_t)10, topicInfo.partitionminbuffersize());
    ASSERT_EQ((uint32_t)256, topicInfo.partitionmaxbuffersize());
    ASSERT_EQ((int)3, topicInfo.partitioninfos_size());

    // test topic simple info
    _sysController->getAllTopicSimpleInfo(&request, &simpleResponse);
    ASSERT_EQ(protocol::ERROR_NONE, simpleResponse.errorinfo().errcode());
    ASSERT_EQ((int)2, simpleResponse.alltopicsimpleinfo_size());
    protocol::TopicSimpleInfo simpleInfo = simpleResponse.alltopicsimpleinfo(0);
    ASSERT_EQ(string("topic_0"), simpleInfo.name());
    ASSERT_EQ(0, simpleInfo.owners_size());
    ASSERT_EQ((uint32_t)3, simpleInfo.partitioncount());
    ASSERT_EQ((uint32_t)1, simpleInfo.runningcount());

    simpleInfo = simpleResponse.alltopicsimpleinfo(1);
    ASSERT_EQ(string("topic_1"), simpleInfo.name());
    ASSERT_EQ(2, simpleInfo.owners_size());
    ASSERT_EQ("Jack", simpleInfo.owners(0));
    ASSERT_EQ("Smith", simpleInfo.owners(1));
    ASSERT_EQ((uint32_t)3, simpleInfo.partitioncount());
    ASSERT_EQ((uint32_t)1, simpleInfo.runningcount());
}

TEST_F(SysControllerTest, testGetAllTopicInfoMutilThread) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    protocol::EmptyRequest request;
    topicCreateRequest.set_partitioncount(3);
    topicCreateRequest.set_partitionminbuffersize(10);
    topicCreateRequest.set_partitionmaxbuffersize(20);
    vector<ThreadPtr> threadVec;
    for (int i = 0; i < 10; i++) {
        threadVec.push_back(Thread::createThread(std::bind(&SysControllerTest::getAllTopicInfo, this)));
    }
    for (int32_t i = 0; i < 400; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        topicCreateRequest.set_topicname(topicName);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        usleep(20000);
    }

    for (int i = 0; i < 10; i++) {
        threadVec[i]->join();
    }
}

void SysControllerTest::getAllTopicInfo() {
    protocol::EmptyRequest request;
    protocol::AllTopicInfoResponse response;
    while (response.alltopicinfo_size() <= 400) {
        _sysController->getAllTopicInfo(&request, &response);
    }
}

TEST_F(SysControllerTest, testFillTopicInfo) {
    {
        TopicCreationRequest request;
        request.set_partitioncount(1);
        TopicInfoPtr topicInfo(new TopicInfo(request));
        protocol::TopicInfo ti;
        TopicInfoRequest inputRequest;
        _sysController->fillTopicInfo(topicInfo, &ti, &inputRequest);
        ASSERT_EQ("", ti.name());
        ASSERT_EQ(1, ti.partitioninfos_size());
        ASSERT_EQ(-1, ti.partitioninfos(0).sessionid());
        ASSERT_EQ(TOPIC_STATUS_WAITING, ti.status());
        ASSERT_EQ(1, ti.partitioncount());
        ASSERT_EQ(0, ti.partitionbuffersize());
        ASSERT_EQ(1, ti.resource());
        ASSERT_EQ(100000, ti.partitionlimit());
        ASSERT_EQ(TOPIC_MODE_NORMAL, ti.topicmode());
        ASSERT_EQ(false, ti.needfieldfilter());
        ASSERT_EQ(-1, ti.obsoletefiletimeinterval());
        ASSERT_EQ(-1, ti.reservedfilecount());
        ASSERT_EQ(false, ti.deletetopicdata());
        ASSERT_EQ(8, ti.partitionminbuffersize());
        ASSERT_EQ(256, ti.partitionmaxbuffersize());
        ASSERT_EQ(50000, ti.maxwaittimeforsecuritycommit());
        ASSERT_EQ(1048576, ti.maxdatasizeforsecuritycommit());
        ASSERT_EQ(false, ti.compressmsg());
        ASSERT_TRUE(ti.compressthres()); // proto bug
        ASSERT_EQ(-1, ti.createtime());
        ASSERT_EQ("", ti.dfsroot());
        ASSERT_EQ("default", ti.topicgroup());
        ASSERT_EQ(0, ti.extenddfsroot_size());
        ASSERT_EQ(-1, ti.topicexpiredtime());
        ASSERT_EQ(1, ti.rangecountinpartition());
        ASSERT_EQ(-1, ti.modifytime());
        ASSERT_EQ(0, ti.owners_size());
        ASSERT_EQ(false, ti.needschema());
        ASSERT_EQ(0, ti.schemaversions_size());
        ASSERT_EQ(false, ti.sealed());
        ASSERT_EQ(TOPIC_TYPE_NORMAL, ti.topictype());
        ASSERT_EQ(0, ti.physictopiclst_size());
        ASSERT_EQ(true, ti.enablettldel());
    }
    {
        TopicCreationRequest request;
        request.set_topicname("test");
        request.set_partitioncount(1);
        request.set_resource(3);
        request.set_partitionlimit(4);
        request.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        request.set_needfieldfilter(true);
        request.set_obsoletefiletimeinterval(5);
        request.set_reservedfilecount(6);
        request.set_deletetopicdata(true);
        request.set_partitionminbuffersize(8);
        request.set_partitionmaxbuffersize(9);
        request.set_maxwaittimeforsecuritycommit(10);
        request.set_maxdatasizeforsecuritycommit(11);
        request.set_compressmsg(true);
        request.set_compressthres(12);
        request.set_createtime(13);
        request.set_dfsroot("dfs:14");
        request.set_topicgroup("15");
        request.add_extenddfsroot("dfs://ex1");
        request.add_extenddfsroot("dfs://ex2");
        request.set_topicexpiredtime(16);
        request.set_rangecountinpartition(17);
        request.set_modifytime(18);
        request.add_owners("owner1");
        request.add_owners("owner2");
        request.set_needschema(true);
        request.add_schemaversions(19);
        request.add_schemaversions(20);
        request.set_timeout(21);
        request.set_sealed(true);
        request.set_topictype(TOPIC_TYPE_PHYSIC);
        request.add_physictopiclst("physic1");
        request.add_physictopiclst("physic2");
        request.set_enablettldel(false);

        TopicInfoPtr topicInfo(new TopicInfo(request, PARTITION_STATUS_STOPPING));
        topicInfo->setCurrentRoleAddress(0, "default##broker_1_0#_#1.0.0.0");
        topicInfo->setSessionId(0, 1670306845);
        protocol::TopicInfo ti;
        TopicInfoRequest inputRequest;
        _sysController->fillTopicInfo(topicInfo, &ti, &inputRequest);
        EXPECT_EQ("test", ti.name());
        EXPECT_EQ(TOPIC_STATUS_STOPPING, ti.status());
        EXPECT_EQ(1, ti.partitioninfos_size());
        EXPECT_EQ(0, ti.partitioninfos(0).id());
        EXPECT_EQ("1.0.0.0", ti.partitioninfos(0).brokeraddress());
        EXPECT_EQ("default##broker_1_0", ti.partitioninfos(0).rolename());
        EXPECT_EQ(1670306845, ti.partitioninfos(0).sessionid());
        EXPECT_EQ(1, ti.partitioncount());
        EXPECT_EQ(3, ti.resource());
        EXPECT_EQ(4, ti.partitionlimit());
        EXPECT_EQ(TOPIC_MODE_MEMORY_ONLY, ti.topicmode());
        EXPECT_EQ(true, ti.needfieldfilter());
        EXPECT_EQ(5, ti.obsoletefiletimeinterval());
        EXPECT_EQ(6, ti.reservedfilecount());
        EXPECT_EQ(true, ti.deletetopicdata());
        EXPECT_EQ(8, ti.partitionminbuffersize());
        EXPECT_EQ(9, ti.partitionmaxbuffersize());
        EXPECT_EQ(10, ti.maxwaittimeforsecuritycommit());
        EXPECT_EQ(11, ti.maxdatasizeforsecuritycommit());
        EXPECT_EQ(true, ti.compressmsg());
        EXPECT_TRUE(ti.compressthres()); // pb bug
        EXPECT_EQ(13, ti.createtime());
        EXPECT_EQ("dfs:14", ti.dfsroot());
        EXPECT_EQ("15", ti.topicgroup());
        EXPECT_EQ("dfs://ex1", ti.extenddfsroot(0));
        EXPECT_EQ("dfs://ex2", ti.extenddfsroot(1));
        EXPECT_EQ(16, ti.topicexpiredtime());
        EXPECT_EQ(17, ti.rangecountinpartition());
        EXPECT_EQ(18, ti.modifytime());
        EXPECT_EQ("owner1", ti.owners(0));
        EXPECT_EQ("owner2", ti.owners(1));
        EXPECT_EQ(true, ti.needschema());
        EXPECT_EQ(19, ti.schemaversions(0));
        EXPECT_EQ(20, ti.schemaversions(1));
        EXPECT_EQ(true, ti.sealed());
        EXPECT_EQ(TOPIC_TYPE_PHYSIC, ti.topictype());
        EXPECT_EQ("physic1", ti.physictopiclst(0));
        EXPECT_EQ("physic2", ti.physictopiclst(1));
        EXPECT_EQ(false, ti.enablettldel());
    }
}

TEST_F(SysControllerTest, testFillTopicSimpleInfo) {
    TopicCreationRequest request;
    uint32_t partCount = 10;
    request.set_topicname("topic_test");
    request.set_partitioncount(partCount);
    request.add_owners("owner1");
    request.add_owners("owner2");
    TopicInfoPtr topicInfo(new TopicInfo(request));
    for (uint32_t idx = 0; idx != partCount; ++idx) {
        topicInfo->setStatus(idx, PartitionStatus(idx % 4));
    }
    protocol::TopicSimpleInfo simpleInfo;
    _sysController->fillTopicSimpleInfo(topicInfo, &simpleInfo);
    ASSERT_EQ("topic_test", simpleInfo.name());
    ASSERT_EQ(partCount, simpleInfo.partitioncount());
    ASSERT_EQ(2, simpleInfo.runningcount());
    ASSERT_EQ(2, simpleInfo.owners_size());
}

TEST_F(SysControllerTest, testCompatible) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    topicCreateRequest.set_partitioncount(6);
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitionbuffersize(31);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    TopicCreationRequest meta;
    _sysController->_zkDataAccessor->getTopicMeta(topicName, meta);
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.partitioncount(), topicCreateRequest.partitioncount());

    ASSERT_EQ(TOPIC_MODE_NORMAL, meta.topicmode());
    ASSERT_TRUE(!meta.needfieldfilter());
    ASSERT_EQ((int64_t)-1, meta.obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)-1, meta.reservedfilecount());
    ASSERT_TRUE(!meta.deletetopicdata());
    ASSERT_EQ(uint32_t(16), meta.partitionminbuffersize());
    ASSERT_EQ(uint32_t(256), meta.partitionmaxbuffersize());
    ASSERT_EQ(uint32_t(31), meta.partitionbuffersize());

    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
    ASSERT_EQ(topicCreateRequest.partitioncount(), topicInfoPtr->getTopicMeta().partitioncount());
    ASSERT_EQ(TOPIC_MODE_NORMAL, topicInfoPtr->getTopicMeta().topicmode());
    ASSERT_TRUE(!topicInfoPtr->getTopicMeta().needfieldfilter());
    ASSERT_EQ((int64_t)-1, topicInfoPtr->getTopicMeta().obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)-1, topicInfoPtr->getTopicMeta().reservedfilecount());
    ASSERT_TRUE(!topicInfoPtr->getTopicMeta().deletetopicdata());
    ASSERT_EQ((uint32_t)16, topicInfoPtr->getTopicMeta().partitionminbuffersize());
    ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());

    TopicInfoRequest topicInfoRequest;
    topicInfoRequest.set_topicname(topicName);
    TopicInfoResponse topicInfoResponse;
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    protocol::TopicInfo ti = topicInfoResponse.topicinfo();

    ASSERT_EQ(TOPIC_MODE_NORMAL, ti.topicmode());
    ASSERT_TRUE(!ti.needfieldfilter());
    ASSERT_EQ((int64_t)-1, ti.obsoletefiletimeinterval());
    ASSERT_EQ((int32_t)-1, ti.reservedfilecount());
    ASSERT_TRUE(!topicInfoPtr->getTopicMeta().deletetopicdata());
    ASSERT_EQ((uint32_t)16, topicInfoPtr->getTopicMeta().partitionminbuffersize());
    ASSERT_EQ((uint32_t)256, topicInfoPtr->getTopicMeta().partitionmaxbuffersize());
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, topicCreateResponse.errorinfo().errcode());
}

TEST_F(SysControllerTest, testGetTopicLastModifyTime) {
    // test for normal situation
    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic/1/file1.meta", "test");
    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic/1/file2.meta", "test");
    sleep(2);
    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic/2/file1.meta", "test");
    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic/2/file2.meta", "test");

    int64_t timestamp = autil::TimeUtility::currentTimeInSeconds();
    uint64_t modifyTime = 0;
    ASSERT_TRUE(_sysController->getTopicLastModifyTime("topic", GET_TEMPLATE_DATA_PATH(), modifyTime));
    ASSERT_EQ(uint64_t(timestamp), modifyTime);

    // test for topic not exist
    ASSERT_FALSE(_sysController->getTopicLastModifyTime("topic1", GET_TEMPLATE_DATA_PATH(), modifyTime));
    // system stop
    _sysController->_stop = true;
    ASSERT_FALSE(_sysController->getTopicLastModifyTime("topic", GET_TEMPLATE_DATA_PATH(), modifyTime));

    _sysController->_deletedTopicMap["topic"] = 12345;
    ASSERT_TRUE(_sysController->getTopicLastModifyTime("topic", GET_TEMPLATE_DATA_PATH(), modifyTime));
    ASSERT_EQ(uint64_t(12345), modifyTime);
}

TEST_F(SysControllerTest, testRemoveOldTopicData) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    string topicName = "topic_1";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(1);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);

    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic_1/1/file1", "test");
    FileSystem::writeFile(GET_TEMPLATE_DATA_PATH() + "/topic_2/1/file1", "test");
    sleep(1);
    _adminConfig.reserveDataHour = 0.00001;
    _adminConfig.dfsRoot = GET_TEMPLATE_DATA_PATH();
    _sysController->removeOldTopicData();
    EXPECT_EQ(EC_FALSE, FileSystem::isExist(GET_TEMPLATE_DATA_PATH() + "/topic_2"));
    EXPECT_EQ(EC_TRUE, FileSystem::isExist(GET_TEMPLATE_DATA_PATH() + "/topic_1"));
}

TEST_F(SysControllerTest, testDoUpdateLeaderInfo) {
    LeaderInfo li;
    AdminInfo *adminInfo = li.add_admininfos();
    adminInfo->set_address("addr");
    adminInfo->set_isprimary(false);
    adminInfo->set_isalive(true);
    _sysController->_leaderInfo = li;
    {
        vector<AdminInfo> adminInfoVec;
        AdminInfo tmpAdminInfo;
        tmpAdminInfo.set_address("addr1");
        tmpAdminInfo.set_isprimary(true);
        tmpAdminInfo.set_isalive(true);
        adminInfoVec.push_back(tmpAdminInfo);
        ASSERT_TRUE(_sysController->doUpdateLeaderInfo(adminInfoVec));
        ASSERT_EQ(int32_t(1), _sysController->_leaderInfo.admininfos_size());
        ASSERT_EQ(string("addr1"), _sysController->_leaderInfo.admininfos(0).address());
        ASSERT_TRUE(_sysController->_leaderInfo.admininfos(0).isprimary());
        ASSERT_TRUE(_sysController->_leaderInfo.admininfos(0).isalive());
    }
    {
        _zkWrapper->close();
        vector<AdminInfo> adminInfoVec;
        AdminInfo tmpAdminInfo;
        tmpAdminInfo.set_address("addr1");
        tmpAdminInfo.set_isprimary(false);
        tmpAdminInfo.set_isalive(false);
        adminInfoVec.push_back(tmpAdminInfo);
        ASSERT_FALSE(_sysController->doUpdateLeaderInfo(adminInfoVec));
        ASSERT_EQ(int32_t(0), _sysController->_leaderInfo.admininfos_size());
    }
}

TEST_F(SysControllerTest, testUpdateLeaderInfo) {
    WorkerMap adminMap;

    RoleInfo roleInfo("role_1", "127.0.0.1", 1111);
    WorkerInfoPtr workerInfoPtr1(new WorkerInfo(roleInfo));
    HeartbeatInfo current;
    string address = "role_1#_#127.0.0.1:1111";
    current.set_address(address);
    current.set_alive(true);
    workerInfoPtr1->setCurrent(current);
    adminMap["role_1"] = workerInfoPtr1;

    RoleInfo roleInfo1("role_2", "127.0.0.2", 1111);
    WorkerInfoPtr workerInfoPtr2(new WorkerInfo(roleInfo1));
    HeartbeatInfo current1;
    string address1 = "role_2#_#127.0.0.2:1111";
    current1.set_address(address1);
    current1.set_alive(true);
    workerInfoPtr2->setCurrent(current1);
    adminMap["role_2"] = workerInfoPtr2;
    AdminConfig adminConfig;
    adminConfig.forceSyncLeaderInfoInterval = -1;
    {
        MockSysController sysController(&adminConfig, nullptr);
        sysController._workerTable._adminWorkerMap = adminMap;

        LeaderInfo li;
        li.set_address("127.0.0.1:1111");
        AdminInfo *adminInfo = li.add_admininfos();
        adminInfo->set_address("127.0.0.1:1111");
        adminInfo->set_isprimary(true);
        adminInfo->set_isalive(true);

        AdminInfo *adminInfo1 = li.add_admininfos();
        adminInfo1->set_address("127.0.0.2:1111");
        adminInfo1->set_isprimary(false);
        adminInfo1->set_isalive(true);

        sysController._leaderInfo = li;

        EXPECT_CALL(sysController, doUpdateLeaderInfo(_)).Times(0);
        sysController.updateLeaderInfo();
    }
    {
        MockSysController sysController(&adminConfig, nullptr);
        sysController._workerTable._adminWorkerMap = adminMap;

        LeaderInfo li;
        li.set_address("127.0.0.1:1111");
        AdminInfo *adminInfo = li.add_admininfos();
        adminInfo->set_address("127.0.0.1:1112");
        adminInfo->set_isprimary(true);
        adminInfo->set_isalive(true);

        AdminInfo *adminInfo1 = li.add_admininfos();
        adminInfo1->set_address("127.0.0.2:1111");
        adminInfo1->set_isprimary(false);
        adminInfo1->set_isalive(true);

        sysController._leaderInfo = li;

        EXPECT_CALL(sysController, doUpdateLeaderInfo(_)).Times(1);
        sysController.updateLeaderInfo();
    }
    // test master version change
    {
        MockSysController sysController(&adminConfig, nullptr);
        sysController._workerTable._adminWorkerMap = adminMap;

        LeaderInfo li;
        li.set_selfmasterversion(111);
        li.set_address("127.0.0.1:1111");
        AdminInfo *adminInfo = li.add_admininfos();
        adminInfo->set_address("127.0.0.1:1111");
        adminInfo->set_isprimary(true);
        adminInfo->set_isalive(true);

        AdminInfo *adminInfo1 = li.add_admininfos();
        adminInfo1->set_address("127.0.0.2:1111");
        adminInfo1->set_isprimary(false);
        adminInfo1->set_isalive(true);

        sysController._leaderInfo = li;

        EXPECT_CALL(sysController, doUpdateLeaderInfo(_)).Times(1);
        sysController.updateLeaderInfo();
    }
}

TEST_F(SysControllerTest, testForceSyncLeaderInfo) {
    AdminConfig adminConfig;
    MockSysController sysController(&adminConfig, nullptr);
    adminConfig.forceSyncLeaderInfoInterval = -1;
    EXPECT_FALSE(sysController.forceSyncLeaderInfo());

    sysController.setPrimary(true);
    adminConfig.forceSyncLeaderInfoInterval = 60 * 1000 * 1000;
    int64_t curTime = TimeUtility::currentTime();
    EXPECT_TRUE(sysController.forceSyncLeaderInfo());
    EXPECT_GE(sysController._forceSyncLeaderInfoTimestamp, curTime + adminConfig.forceSyncLeaderInfoInterval);
    EXPECT_FALSE(sysController.forceSyncLeaderInfo());

    sysController._forceSyncLeaderInfoTimestamp = curTime;
    EXPECT_TRUE(sysController.forceSyncLeaderInfo());

    sysController._forceSyncLeaderInfoTimestamp = curTime;
    sysController.setPrimary(false);
    EXPECT_FALSE(sysController.forceSyncLeaderInfo());
}

TEST_F(SysControllerTest, testReadLeaderInfoPb) {
    std::string zkPath = GET_TEMPLATE_DATA_PATH() + "/admin";
    FileSystem::remove(zkPath);
    MockSysController sysController(nullptr, nullptr);
    LeaderInfo leaderInfo;
    ASSERT_FALSE(sysController.readLeaderInfo(zkPath + "/leader_info", false, leaderInfo));

    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(zkPath, true));
    std::string leaderInfoConstStr;
    _leaderInfoConst.SerializeToString(&leaderInfoConstStr);
    FileSystem::writeFile(zkPath + "/leader_info", leaderInfoConstStr);

    ASSERT_TRUE(sysController.readLeaderInfo(zkPath + "/leader_info", false, leaderInfo));
    EXPECT_EQ(_leaderInfoConst, leaderInfo);
}

TEST_F(SysControllerTest, testReadLeaderInfoJson) {
    std::string zkPath = GET_TEMPLATE_DATA_PATH() + "/syncLeaderInfo/admin";
    FileSystem::remove(zkPath);
    MockSysController sysController(nullptr, nullptr);
    LeaderInfo leaderInfo;
    ASSERT_FALSE(sysController.readLeaderInfo(zkPath + "/leader_info", true, leaderInfo));

    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(zkPath, true));
    auto leaderInfoConstStr = http_arpc::ProtoJsonizer::toJsonString(_leaderInfoConst);
    FileSystem::writeFile(zkPath + "/leader_info", leaderInfoConstStr);

    ASSERT_TRUE(sysController.readLeaderInfo(zkPath + "/leader_info", true, leaderInfo));
    EXPECT_EQ(_leaderInfoConst, leaderInfo);
}

TEST_F(SysControllerTest, testWriteLeaderInfo) {
    std::string zkPath = GET_TEMPLATE_DATA_PATH() + "/syncLeaderInfo/write_test";
    FileSystem::remove(zkPath);
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(zkPath, true));
    std::string filePath = zkPath + "/data";
    FileSystem::writeFile(filePath, "aaa");
    MockSysController sysController(nullptr, nullptr);
    sysController.writeLeaderInfo(filePath, "bbb");
    std::string content;
    FileSystem::readFile(filePath, content);
    EXPECT_EQ("bbb", content);
    sysController.writeLeaderInfo(filePath, "ccc");
    content.clear();
    FileSystem::readFile(filePath, content);
    EXPECT_EQ("ccc", content);

    FileSystem::remove(filePath);
    sysController.writeLeaderInfo(filePath, "ddd");
    content.clear();
    FileSystem::readFile(filePath, content);
    EXPECT_EQ("ddd", content);
}

TEST_F(SysControllerTest, testCheckLeaderInfoDiff) {
    std::string zkPath = GET_TEMPLATE_DATA_PATH() + "/syncLeaderInfo/leader";
    FileSystem::remove(zkPath);
    std::string filePath = zkPath + "/leader_info";
    ASSERT_TRUE(_sysController->checkLeaderInfoDiff(filePath, false));

    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(zkPath, true));
    std::string content("abcdefg");
    FileSystem::writeFile(filePath, content);
    ASSERT_TRUE(_sysController->checkLeaderInfoDiff(filePath, false));

    LeaderInfo leaderInfo;
    leaderInfo.set_address("127.0.0.1");
    leaderInfo.set_sysstop(false);
    leaderInfo.set_starttimesec(13579);
    leaderInfo.set_selfmasterversion(246810);
    leaderInfo.SerializeToString(&content);
    FileSystem::writeFile(filePath, content);
    ASSERT_TRUE(_sysController->checkLeaderInfoDiff(filePath, false));

    _sysController->_leaderInfo = leaderInfo;
    ASSERT_FALSE(_sysController->checkLeaderInfoDiff(filePath, false));
}

#define FSLIB_MOCK_FORWARD_FUNCTION_NAME "mockForward"
typedef std::function<fslib::ErrorCode(const std::string &path, const std::string &args, std::string &output)>
    ForwardFunc;
typedef void (*MockForwardFunc)(const std::string &, const ForwardFunc &);

TEST_F(SysControllerTest, testUpdateMasterStatusStatError) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "100";
        return fslib::EC_BADF;
    });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    string mockInline("mock://na61/swift/inline");
    ASSERT_FALSE(sysController.updateMasterStatus(mockInline));
}

TEST_F(SysControllerTest, testUpdateMasterStatusConvertError) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "aaa";
        return fslib::EC_OK;
    });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    string mockInline("mock://na61/swift/inline");
    ASSERT_FALSE(sysController.updateMasterStatus(mockInline));
}

TEST_F(SysControllerTest, testUpdateMasterStatusVersionSkip) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "99";
        return fslib::EC_OK;
    });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    sysController._selfMasterVersion = 100;
    sysController.setMaster(true);
    string mockInline("mock://na61/swift/inline");
    ASSERT_FALSE(sysController.updateMasterStatus(mockInline));
    ASSERT_TRUE(sysController.isMaster());
}

TEST_F(SysControllerTest, testUpdateMasterStatusUpdatePanguError) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "99";
        return fslib::EC_OK;
    });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    sysController._selfMasterVersion = 100;
    sysController.setMaster(false);
    string mockInline("mock://na61/swift/inline");
    ASSERT_FALSE(sysController.updateMasterStatus(mockInline));
    ASSERT_FALSE(sysController.isMaster());
}

TEST_F(SysControllerTest, testUpdateMasterStatusStatSkipUpdate) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "100";
        return ::fslib::EC_OK;
    });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    sysController._selfMasterVersion = 100;
    sysController.setMaster(false);
    string mockInline("mock://na61/swift/inline");
    ASSERT_TRUE(sysController.updateMasterStatus(mockInline, false));
    ASSERT_TRUE(sysController.isMaster());
}

TEST_F(SysControllerTest, testUpdateMasterStatusUpdateSucc) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    mockForward("pangu_StatInlinefile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
        output = "100";
        return fslib::EC_OK;
    });
    mockForward("pangu_UpdateInlinefileCAS",
                [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                    const string &expectArgs = StringUtil::formatString("100%c101", '\0');
                    output = "100";
                    return (args == expectArgs) ? fslib::EC_OK : fslib::EC_FALSE;
                });
    AdminConfig adminConfig;
    adminConfig.backup = true;
    MockSysController sysController(&adminConfig, nullptr);
    sysController._selfMasterVersion = 101;
    sysController.setMaster(false);
    string mockInline("mock://na61/swift/inline");
    ASSERT_TRUE(sysController.updateMasterStatus(mockInline));
    ASSERT_TRUE(sysController.isMaster());
}

TEST_F(SysControllerTest, testPublishLeaderInfo) {
    std::string zkPath = GET_TEMPLATE_DATA_PATH() + "/syncLeaderInfo/leader_info";
    string filePath = PathDefine::getLeaderInfoFile(zkPath);
    string jsonFilePath = PathDefine::getLeaderInfoJsonFile(zkPath);
    {
        FileSystem::remove(filePath);
        FileSystem::remove(jsonFilePath);
        AdminConfig adminConfig;
        MockSysController sysController(&adminConfig, nullptr);
        vector<string> adminInfoPathVec;
        sysController.init();
        sysController._adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        sysController.setPrimary(true);
        EXPECT_CALL(sysController, checkLeaderInfoDiff(_, _)).Times(0);
        sysController.publishLeaderInfo();
    }
    {
        FileSystem::remove(filePath);
        FileSystem::remove(jsonFilePath);
        AdminConfig adminConfig;
        MockSysController sysController(&adminConfig, nullptr);
        vector<string> adminInfoPathVec;
        sysController.init();
        adminInfoPathVec.emplace_back(zkPath);
        sysController._adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        sysController.setPrimary(true);
        sysController._adminConfig->backup = false;
        sysController._leaderInfo = _leaderInfoConst;
        FileSystem::writeFile(filePath, "aaa");
        FileSystem::writeFile(jsonFilePath, "bbb");
        EXPECT_CALL(sysController, checkLeaderInfoDiff(_, _)).Times(2).WillRepeatedly(Return(false));
        sysController.publishLeaderInfo();
        std::string content;
        FileSystem::readFile(filePath, content);
        EXPECT_EQ("aaa", content);
        content.clear();
        FileSystem::readFile(jsonFilePath, content);
        EXPECT_EQ("bbb", content);
    }
    {
        FileSystem::remove(filePath);
        FileSystem::remove(jsonFilePath);
        AdminConfig adminConfig;
        MockSysController sysController(&adminConfig, nullptr);
        vector<string> adminInfoPathVec;
        sysController.init();
        adminInfoPathVec.emplace_back(zkPath);
        sysController._adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        sysController.setPrimary(true);
        sysController._adminConfig->backup = false;
        sysController._leaderInfo = _leaderInfoConst;
        FileSystem::writeFile(filePath, "aaa");
        FileSystem::writeFile(jsonFilePath, "bbb");

        EXPECT_CALL(sysController, checkLeaderInfoDiff(_, _)).WillOnce(Return(true)).WillOnce(Return(false));
        sysController.publishLeaderInfo();
        std::string content;
        FileSystem::readFile(filePath, content);
        std::string leaderInfoConstStr;
        _leaderInfoConst.SerializeToString(&leaderInfoConstStr);
        EXPECT_EQ(leaderInfoConstStr, content);
        content.clear();
        FileSystem::readFile(jsonFilePath, content);
        EXPECT_EQ("bbb", content);
    }
    {
        FileSystem::remove(filePath);
        FileSystem::remove(jsonFilePath);
        AdminConfig adminConfig;
        MockSysController sysController(&adminConfig, nullptr);
        vector<string> adminInfoPathVec;
        sysController.init();
        adminInfoPathVec.emplace_back(zkPath);
        sysController._adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        sysController.setPrimary(true);
        sysController._adminConfig->backup = false;
        sysController._leaderInfo = _leaderInfoConst;
        FileSystem::writeFile(filePath, "aaa");
        FileSystem::writeFile(jsonFilePath, "bbb");

        EXPECT_CALL(sysController, checkLeaderInfoDiff(_, _)).WillOnce(Return(false)).WillOnce(Return(true));
        sysController.publishLeaderInfo();
        std::string content;
        FileSystem::readFile(filePath, content);
        EXPECT_EQ("aaa", content);
        content.clear();
        FileSystem::readFile(jsonFilePath, content);
        auto leaderInfoConstStr = http_arpc::ProtoJsonizer::toJsonString(_leaderInfoConst);
        EXPECT_EQ(leaderInfoConstStr, content);
    }
    {
        FileSystem::remove(filePath);
        FileSystem::remove(jsonFilePath);
        AdminConfig adminConfig;
        MockSysController sysController(&adminConfig, nullptr);
        vector<string> adminInfoPathVec;
        sysController.init();
        adminInfoPathVec.emplace_back(zkPath);
        sysController._adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        sysController.setPrimary(true);
        sysController._adminConfig->backup = false;
        sysController._leaderInfo = _leaderInfoConst;
        FileSystem::writeFile(filePath, "aaa");
        FileSystem::writeFile(jsonFilePath, "bbb");

        EXPECT_CALL(sysController, checkLeaderInfoDiff(_, _)).WillOnce(Return(true)).WillOnce(Return(true));
        sysController.publishLeaderInfo();
        std::string content;
        FileSystem::readFile(filePath, content);
        std::string leaderInfoConstStr;
        _leaderInfoConst.SerializeToString(&leaderInfoConstStr);
        EXPECT_EQ(leaderInfoConstStr, content);
        content.clear();
        FileSystem::readFile(jsonFilePath, content);
        leaderInfoConstStr = http_arpc::ProtoJsonizer::toJsonString(_leaderInfoConst);
        EXPECT_EQ(leaderInfoConstStr, content);
    }
}

TEST_F(SysControllerTest, testTransferPartition) {
    _sysController->setPrimary(true);
    {
        PartitionTransferRequest request;
        PartitionTransferResponse response;
        PartitionTransfer *info = request.add_transferinfo();
        info->set_brokerrolename("role1");
        info->set_ratio(1.3);
        _sysController->transferPartition(&request, &response);
        ASSERT_EQ(protocol::ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
        ASSERT_EQ(int64_t(0), _sysController->_adjustBeginTime);
    }
    {
        PartitionTransferRequest request;
        PartitionTransferResponse response;
        PartitionTransfer *info = request.add_transferinfo();
        info->set_brokerrolename("role1");
        info->set_ratio(-0.3);
        _sysController->transferPartition(&request, &response);
        ASSERT_EQ(protocol::ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
        ASSERT_EQ(int64_t(0), _sysController->_adjustBeginTime);
    }
    {
        PartitionTransferRequest request;
        PartitionTransferResponse response;
        PartitionTransfer *info = request.add_transferinfo();
        info->set_brokerrolename("role1");
        info->set_ratio(0.3);
        _sysController->transferPartition(&request, &response);
        ASSERT_EQ(protocol::ERROR_NONE, response.errorinfo().errcode());
        int64_t time = TimeUtility::currentTime() - _sysController->_adjustBeginTime;
        ASSERT_EQ(size_t(1), _sysController->_adjustWorkerResourceMap.size());
        ASSERT_FLOAT_EQ(0.3, _sysController->_adjustWorkerResourceMap["role1"]);
        ASSERT_TRUE(time < 1000 * 1000);
        ASSERT_FALSE(_sysController->_clearCurrentTask);
        ASSERT_TRUE(_sysController->_transferGroupName.empty());
    }
}
TEST_F(SysControllerTest, testTransferPartition2) {
    _sysController->setPrimary(true);
    {
        PartitionTransferRequest request;
        PartitionTransferResponse response;
        request.set_groupname("abc");
        _sysController->transferPartition(&request, &response);
        ASSERT_EQ(protocol::ERROR_NONE, response.errorinfo().errcode());
        int64_t time = TimeUtility::currentTime() - _sysController->_adjustBeginTime;
        ASSERT_EQ(size_t(0), _sysController->_adjustWorkerResourceMap.size());
        ASSERT_TRUE(time < 1000 * 1000);
        ASSERT_TRUE(_sysController->_clearCurrentTask);
        ASSERT_EQ("abc", _sysController->_transferGroupName);
    }
}

TEST_F(SysControllerTest, testDeleteExpiredTopic) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    string topicName1 = "topic_1";
    topicCreateRequest.set_topicname(topicName1);
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_topicexpiredtime(3);
    TopicCreationRequest topicCreateRequest2;
    string topicName2 = "topic_2";
    topicCreateRequest2.set_topicname(topicName2);
    topicCreateRequest2.set_partitioncount(1);

    TopicCreationRequest topicCreateRequest3;
    string topicName3 = "topic_3";
    topicCreateRequest3.set_topicname(topicName3);
    topicCreateRequest3.set_partitioncount(1);
    topicCreateRequest3.set_topicexpiredtime(-1);

    TopicCreationRequest topicCreateRequest4;
    string topicName4 = "topic_4";
    topicCreateRequest4.set_topicname(topicName4);
    topicCreateRequest4.set_partitioncount(1);
    topicCreateRequest4.set_topicexpiredtime(TimeUtility::currentTimeInSeconds() + 5);

    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    _sysController->createTopic(&topicCreateRequest2, &topicCreateResponse);
    _sysController->createTopic(&topicCreateRequest3, &topicCreateResponse);
    _sysController->createTopic(&topicCreateRequest4, &topicCreateResponse);
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName1));
    _sysController->deleteExpiredTopic();
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName1));
    ASSERT_EQ(0, _sysController->_deletedTopicMap.size());
    usleep(3 * 1000 * 1000);
    _sysController->deleteExpiredTopic();
    ASSERT_FALSE(_sysController->_topicTable.findTopic(topicName1));
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName2));
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName3));
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName4));
    ASSERT_EQ(1, _sysController->_deletedTopicMap.size());
    usleep(3 * 1000 * 1000);
    _sysController->deleteExpiredTopic();
    ASSERT_FALSE(_sysController->_topicTable.findTopic(topicName1));
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName2));
    ASSERT_TRUE(_sysController->_topicTable.findTopic(topicName3));
    ASSERT_FALSE(_sysController->_topicTable.findTopic(topicName4));
    ASSERT_EQ(2, _sysController->_deletedTopicMap.size());
}

TEST_F(SysControllerTest, testDeleteExpiredTopic_LP) {
    _sysController->setPrimary(true);
    TopicBatchCreationRequest createRequest;
    TopicBatchCreationResponse createResponse;
    TopicCreationRequest meta;
    meta.set_partitioncount(2);
    meta.set_topicname("topic_n");
    meta.set_topictype(TOPIC_TYPE_NORMAL);
    meta.set_topicexpiredtime(-1);
    *createRequest.add_topicrequests() = meta;

    int64_t curTimeSec = TimeUtility::currentTimeInSeconds();
    meta.set_topicname("topic_pl");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    *meta.add_physictopiclst() = "topic_pl-111-1";
    meta.set_sealed(true);
    meta.set_topicexpiredtime(curTimeSec + 1);
    *createRequest.add_topicrequests() = meta;
    meta.clear_physictopiclst();
    meta.set_topicname("topic_pl-111-1");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    meta.set_sealed(false);
    meta.set_enablettldel(false);
    meta.set_topicexpiredtime(curTimeSec + 1);
    *createRequest.add_topicrequests() = meta;

    meta.set_topicname("topic_pl2");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.set_topicexpiredtime(curTimeSec + 1);
    *createRequest.add_topicrequests() = meta;

    meta.set_topicname("topic_l");
    meta.set_topictype(TOPIC_TYPE_LOGIC);
    meta.set_topicexpiredtime(-1);
    *meta.add_physictopiclst() = "topic_l-111-1";
    *meta.add_physictopiclst() = "topic_l-222-2";
    *createRequest.add_topicrequests() = meta;
    meta.clear_physictopiclst();
    meta.set_topicname("topic_l-111-1");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    meta.set_sealed(true);
    meta.set_enablettldel(true);
    meta.set_topicexpiredtime(curTimeSec + 1);
    *createRequest.add_topicrequests() = meta;
    meta.set_topicname("topic_l-222-2");
    meta.set_topictype(TOPIC_TYPE_PHYSIC);
    meta.set_topicexpiredtime(-1);
    meta.set_enablettldel(false);
    *createRequest.add_topicrequests() = meta;
    _sysController->createTopicBatch(&createRequest, &createResponse);
    ASSERT_EQ(ERROR_NONE, createResponse.errorinfo().errcode());
    auto &topicMap = _sysController->_topicTable._topicMap;
    auto &deletedMap = _sysController->_deletedTopicMap;
    ASSERT_EQ(7, topicMap.size());

    _sysController->deleteExpiredTopic();
    ASSERT_EQ(7, topicMap.size());   // no topic update
    ASSERT_EQ(0, deletedMap.size()); // no topic delete

    usleep(2 * 1000 * 1000);
    _sysController->deleteExpiredTopic();
    ASSERT_EQ(1, deletedMap.size());
    ASSERT_TRUE(deletedMap.end() != deletedMap.find("topic_l-111-1"));
    ASSERT_EQ(6, topicMap.size());
    auto iter = topicMap.find("topic_pl");
    ASSERT_TRUE(topicMap.end() != iter);
    ASSERT_EQ(TOPIC_TYPE_LOGIC, iter->second->_topicMeta.topictype());
    ASSERT_EQ(1, iter->second->_topicMeta.physictopiclst_size());
    ASSERT_EQ("topic_pl-111-1", iter->second->_topicMeta.physictopiclst(0));
    ASSERT_EQ(-1, iter->second->_topicMeta.topicexpiredtime());
    ASSERT_TRUE(iter->second->_topicMeta.enablettldel());

    iter = topicMap.find("topic_pl2"); // not change
    ASSERT_TRUE(topicMap.end() != iter);
    ASSERT_EQ(TOPIC_TYPE_LOGIC_PHYSIC, iter->second->_topicMeta.topictype());
    ASSERT_EQ(0, iter->second->_topicMeta.physictopiclst_size());
    ASSERT_EQ(curTimeSec + 1, iter->second->_topicMeta.topicexpiredtime());

    iter = topicMap.find("topic_l");
    ASSERT_TRUE(topicMap.end() != iter);
    ASSERT_EQ(TOPIC_TYPE_LOGIC, iter->second->_topicMeta.topictype());
    ASSERT_EQ(1, iter->second->_topicMeta.physictopiclst_size());
    ASSERT_EQ("topic_l-222-2", iter->second->_topicMeta.physictopiclst(0));
    ASSERT_EQ(-1, iter->second->_topicMeta.topicexpiredtime());

    iter = topicMap.find("topic_l-111-1");
    ASSERT_TRUE(topicMap.end() == iter);

    iter = topicMap.find("topic_l-222-2");
    ASSERT_TRUE(topicMap.end() != iter);
    ASSERT_EQ(TOPIC_TYPE_PHYSIC, iter->second->_topicMeta.topictype());
    ASSERT_EQ(-1, iter->second->_topicMeta.topicexpiredtime());
    ASSERT_TRUE(iter->second->_topicMeta.enablettldel());
}

TEST_F(SysControllerTest, testUpdateDfsRoot) {
    TopicMetas topicMetas;
    TopicCreationRequest topicCreateRequest;
    _sysController->setPrimary(true);
    string topicName0 = "topic_0";
    topicCreateRequest.set_topicname(topicName0);
    topicCreateRequest.set_partitioncount(1);
    TopicCreationRequest topicCreateRequest1;
    *topicMetas.add_topicmetas() = topicCreateRequest;
    string topicName1 = "topic_1";
    topicCreateRequest1.set_topicname(topicName1);
    topicCreateRequest1.set_partitioncount(1);
    topicCreateRequest1.set_dfsroot("hdfs://topic1_specify");
    *topicCreateRequest1.add_extenddfsroot() = "hdfs://xxx/extend2";
    *topicMetas.add_topicmetas() = topicCreateRequest1;
    TopicCreationRequest topicCreateRequest2;
    string topicName2 = "topic_2";
    topicCreateRequest2.set_topicname(topicName2);
    topicCreateRequest2.set_partitioncount(1);
    topicCreateRequest2.set_dfsroot("hdfs://xxx");
    *topicCreateRequest2.add_extenddfsroot() = "hdfs://xxx/extend";
    *topicCreateRequest2.add_extenddfsroot() = "hdfs://xxx/extend2";
    *topicMetas.add_topicmetas() = topicCreateRequest2;
    TopicCreationRequest topicCreateRequest3;
    string topicName3 = "topic_3";
    topicCreateRequest3.set_topicname(topicName3);
    topicCreateRequest3.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    *topicMetas.add_topicmetas() = topicCreateRequest3;
    TopicCreationRequest topicCreateRequest4;
    string topicName4 = "topic_4";
    topicCreateRequest4.set_topicname(topicName2);
    topicCreateRequest4.set_partitioncount(1);
    topicCreateRequest4.set_dfsroot("hdfs://xxx");
    *topicCreateRequest4.add_extenddfsroot() = "hdfs://xxx/extend";
    *topicCreateRequest4.add_extenddfsroot() = "hdfs://xxx";
    *topicMetas.add_topicmetas() = topicCreateRequest4;

    { // normal, only delete
        AdminConfig conf;
        TopicMetas metasTmp = topicMetas;
        conf.dfsRoot = "hdfs://xxx";
        conf.extendDfsRoot = "hdfs://xxx/extend";
        conf.todelDfsRoot = "hdfs://xxx/extend2";
        _sysController->_adminConfig = &conf;
        _sysController->updateDfsRoot(metasTmp);

        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(0).dfsroot());
        ASSERT_EQ(conf.extendDfsRoot + ";", getExtendDfsRoot(metasTmp.topicmetas(0)));
        ASSERT_EQ(string("hdfs://topic1_specify"), metasTmp.topicmetas(1).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(1)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(2).dfsroot());
        ASSERT_EQ(conf.extendDfsRoot + ";", getExtendDfsRoot(metasTmp.topicmetas(2)));
        ASSERT_EQ(string(""), metasTmp.topicmetas(3).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(3)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(4).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend;"), getExtendDfsRoot(metasTmp.topicmetas(4)));
    }
    { // migrate hdfs
        AdminConfig conf;
        TopicMetas metasTmp = topicMetas;
        conf.dfsRoot = "hdfs://yyy";
        conf.extendDfsRoot = "hdfs://xxx";
        conf.todelDfsRoot = "hdfs://xxx/extend2";
        _sysController->_adminConfig = &conf;
        _sysController->updateDfsRoot(metasTmp);

        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(0).dfsroot());
        ASSERT_EQ(conf.extendDfsRoot + ";", getExtendDfsRoot(metasTmp.topicmetas(0)));
        ASSERT_EQ(string("hdfs://topic1_specify"), metasTmp.topicmetas(1).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(1)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(2).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend;hdfs://xxx;"), getExtendDfsRoot(metasTmp.topicmetas(2)));
        ASSERT_EQ(string(""), metasTmp.topicmetas(3).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(3)));
        ASSERT_EQ(string(""), metasTmp.topicmetas(3).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(3)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(4).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend;hdfs://xxx;"), getExtendDfsRoot(metasTmp.topicmetas(4)));
    }
    { // migrate and delete
        AdminConfig conf;
        TopicMetas metasTmp = topicMetas;
        conf.dfsRoot = "hdfs://yyy";
        conf.extendDfsRoot = "hdfs://topic1_specify";
        conf.todelDfsRoot = "hdfs://xxx";
        _sysController->_adminConfig = &conf;
        _sysController->updateDfsRoot(metasTmp);

        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(0).dfsroot());
        ASSERT_EQ(conf.extendDfsRoot + ";", getExtendDfsRoot(metasTmp.topicmetas(0)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(1).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend2;hdfs://topic1_specify;"), getExtendDfsRoot(metasTmp.topicmetas(1)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(2).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend;hdfs://xxx/extend2;"), getExtendDfsRoot(metasTmp.topicmetas(2)));
        ASSERT_EQ(string(""), metasTmp.topicmetas(3).dfsroot());
        ASSERT_EQ(string(""), getExtendDfsRoot(metasTmp.topicmetas(3)));
        ASSERT_EQ(conf.dfsRoot, metasTmp.topicmetas(4).dfsroot());
        ASSERT_EQ(string("hdfs://xxx/extend;"), getExtendDfsRoot(metasTmp.topicmetas(4)));
    }
}

string SysControllerTest::getExtendDfsRoot(const TopicCreationRequest &meta) {
    string edfs;
    for (auto idx = 0; idx < meta.extenddfsroot_size(); ++idx) {
        edfs += meta.extenddfsroot(idx) + ";";
    }
    return edfs;
}

template <typename ProtoClass>
bool parseProto(const string &data, ProtoClass &protoValue, bool compress = true, bool json = false) {
    string content(data);
    if (compress) {
        autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
        std::string decompressedData;
        if (!compressor.decompress(content, decompressedData)) {
            return false;
        }
        content = decompressedData;
    }
    if (json) {
        if (!http_arpc::ProtoJsonizer::fromJsonString(content, &protoValue)) {
            return false;
        }
    } else {
        if (!protoValue.ParseFromString(content)) {
            protoValue.Clear();
            return false;
        }
    }
    return true;
}

TEST_F(SysControllerTest, testBackTopicMetas) {
    // SWIFT_ROOT_LOG_SETLEVEL(INFO);
    {
        SysController sysController(&_adminConfig, nullptr);
        sysController.setPrimary(true);
        sysController.init();
        ASSERT_TRUE(sysController._zkDataAccessor->init(_zkWrapper, _zkRoot));

        sysController._adminConfig->backMetaPath = "";
        ASSERT_FALSE(_sysController->backTopicMetas());

        string content("abcdef");
        ASSERT_EQ(EC_OK, FileSystem::writeFile(_zkRoot + "topic_meta", content));

        string metaBackupPath(GET_TEMPLATE_DATA_PATH() + "/backup");
        FileSystem::remove(metaBackupPath);
        sysController._adminConfig->backMetaPath = metaBackupPath;
        ASSERT_TRUE(_sysController->backTopicMetas());

        string metaContent;
        ASSERT_TRUE(_sysController->_zkDataAccessor->readTopicMetas(metaContent));
        ASSERT_EQ(content, metaContent);
    }
    { // real case
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName("topicTest");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(6);
        SysController sysController(&_adminConfig, nullptr);
        sysController.setPrimary(true);
        sysController.init();
        ASSERT_TRUE(sysController._zkDataAccessor->init(_zkWrapper, _zkRoot));
        sysController.createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
        ASSERT_TRUE(sysController.loadTopicInfo());

        sysController._adminConfig->backMetaPath = "";
        ASSERT_FALSE(_sysController->backTopicMetas());

        string metaBackupPath(GET_TEMPLATE_DATA_PATH() + "/backup");
        FileSystem::remove(metaBackupPath);
        sysController._adminConfig->backMetaPath = metaBackupPath;
        ASSERT_TRUE(_sysController->backTopicMetas());

        string metaContent;
        ASSERT_TRUE(_sysController->_zkDataAccessor->readTopicMetas(metaContent));
        protocol::TopicMetas topicMetas;
        ASSERT_TRUE(parseProto<TopicMetas>(metaContent, topicMetas));
        cout << topicMetas.ShortDebugString() << endl;
        ASSERT_EQ(1, topicMetas.topicmetas_size());
        ASSERT_EQ(topicName, topicMetas.topicmetas(0).topicname());
        ASSERT_EQ(6, topicMetas.topicmetas(0).partitioncount());
    }
    { // call backTopicMetas many times, same topic
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName("topicTest");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(6);
        SysController sysController(&_adminConfig, nullptr);
        sysController.setPrimary(true);
        sysController.init();
        ASSERT_TRUE(sysController._zkDataAccessor->init(_zkWrapper, _zkRoot));
        sysController.createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());

        string metaBackupPath(GET_TEMPLATE_DATA_PATH() + "/backup");
        FileSystem::remove(metaBackupPath);
        sysController._adminConfig->backMetaPath = metaBackupPath;
        sysController._adminConfig->reserveBackMetaCount = 10;
        ASSERT_TRUE(_sysController->backTopicMetas());
        for (size_t count = 0; count < 30; ++count) {
            ASSERT_FALSE(_sysController->backTopicMetas());
            vector<string> metaFiles;
            ASSERT_EQ(fslib::EC_OK, FileSystem::listDir(metaBackupPath, metaFiles));
            ASSERT_EQ(1, metaFiles.size());
        }
    }
    { // call backTopicMetas many times, topic will change
        TopicCreationRequest topicCreateRequest;
        TopicCreationResponse topicCreateResponse;
        string topicName("topicTest");
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(6);
        SysController sysController(&_adminConfig, nullptr);
        sysController.setPrimary(true);
        sysController.init();
        ASSERT_TRUE(sysController._zkDataAccessor->init(_zkWrapper, _zkRoot));
        string metaBackupPath(GET_TEMPLATE_DATA_PATH() + "/backup");
        FileSystem::remove(metaBackupPath);
        sysController._adminConfig->backMetaPath = metaBackupPath;
        sysController._adminConfig->reserveBackMetaCount = 10;
        ASSERT_FALSE(_sysController->backTopicMetas());
        for (size_t count = 0; count < 10; ++count) {
            topicCreateRequest.set_topicname(topicName + std::to_string(count));
            sysController.createTopic(&topicCreateRequest, &topicCreateResponse);
            ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
            ASSERT_TRUE(_sysController->backTopicMetas());
            vector<string> metaFiles;
            ASSERT_EQ(fslib::EC_OK, FileSystem::listDir(metaBackupPath, metaFiles));
            ASSERT_EQ(count + 1, metaFiles.size());
            sleep(1);
        }
    }
}

void SysControllerTest::assertReserveCount(uint32_t inputCount,
                                           uint32_t reserveCount,
                                           uint32_t expectCount,
                                           uint32_t expectStartIndex) {
    SysController sysController(nullptr, nullptr);
    string backPath(GET_TEMPLATE_DATA_PATH() + "/backup/");
    FileSystem::remove(backPath);
    uint32_t original = 13175500, tt = original;
    for (size_t count = 0; count < inputCount; ++count) {
        string aa = std::to_string(++tt);
        string fileName("topic_meta_202001" + aa);
        ASSERT_EQ(EC_OK, FileSystem::writeFile(backPath + fileName, "aaa"));
        sysController.deleteObsoleteMetaFiles(backPath, reserveCount);
    }

    vector<string> expect;
    uint32_t last = original + expectStartIndex;
    for (size_t count = 0; count < expectCount; ++count) {
        string aa = std::to_string(++last);
        expect.emplace_back("topic_meta_202001" + aa);
    }

    vector<string> actual;
    ASSERT_EQ(fslib::EC_OK, FileSystem::listDir(backPath, actual));
    std::sort(actual.begin(), actual.end(), std::less<string>());
    ASSERT_EQ(expect, actual);
}

TEST_F(SysControllerTest, testDeleteObsoleteMetaFiles) {
    assertReserveCount(99, 100, 99, 0);
    assertReserveCount(100, 100, 100, 0);
    assertReserveCount(101, 100, 101, 0);
    assertReserveCount(199, 100, 199, 0);
    assertReserveCount(200, 100, 100, 100);
    assertReserveCount(400, 200, 200, 200);
}

TEST_F(SysControllerTest, testRegisterSchema) {
    RegisterSchemaRequest request;
    RegisterSchemaResponse response;

    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_NOT_LEADER, response.errorinfo().errcode());

    // topic & schema empty
    _sysController->setPrimary(true);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // not set schema
    request.set_topicname("topica");
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // not set topic
    request.clear_topicname();
    request.set_schema("schema");
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // set all, but schema parse fail
    request.set_topicname("topica");
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());

    // support schema
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    topicCreateRequest.set_topicname("topica");
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_needschema(true);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    // invalid schema
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
    string schemaStr(
        R"json({"topic":"topica","fields":[{"name":"CMD","type":"string"},{"name":"shop_id","type":"string"}]})json");
    FieldSchema fschema;
    fschema.fromJsonString(schemaStr);
    uint32_t version = fschema.calcVersion();
    request.set_schema(schemaStr);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    TopicCreationRequest meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta("topica", meta));
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.needschema(), topicCreateRequest.needschema());
    ASSERT_EQ(1, meta.schemaversions_size());
    ASSERT_EQ(version, meta.schemaversions(0));

    // version already exist
    response.set_version(0);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(version, response.version());
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_ALREADY_EXIST, response.errorinfo().errcode());

    vector<uint32_t> expectVersions;
    expectVersions.push_back(version);

    // too many versions
    sleep(1);
    size_t pos = schemaStr.find("CMD");
    pos += 2;
    for (uint32_t count = 0; count < 9; ++count) {
        schemaStr[pos] = 'F' + count;
        request.set_schema(schemaStr);
        fschema.fromJsonString(schemaStr);
        expectVersions.push_back(fschema.calcVersion());
        _sysController->registerSchema(&request, &response);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    }
    schemaStr[pos] = 'E';
    request.set_schema(schemaStr);
    fschema.fromJsonString(schemaStr);
    expectVersions[0] = fschema.calcVersion();
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_VERSION_EXCEED, response.errorinfo().errcode());

    // cover version
    request.set_cover(true);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta("topica", meta));
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.needschema(), topicCreateRequest.needschema());
    ASSERT_EQ(10, meta.schemaversions_size());
    TopicInfoPtr infop = _sysController->_topicTable.findTopic("topica");
    EXPECT_TRUE(infop != nullptr);
    for (uint32_t index = 0; index < 10; ++index) {
        ASSERT_EQ(expectVersions[index], meta.schemaversions(index));
        ASSERT_EQ(expectVersions[index], infop->getTopicMeta().schemaversions(index));
    }

    // zk write fail
    _zkWrapper->close();
    schemaStr[pos] = 'Z';
    request.set_schema(schemaStr);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_OPERATION_FAILED, response.errorinfo().errcode());
    infop = _sysController->_topicTable.findTopic("topica");
    ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta("topica", meta));
    EXPECT_TRUE(infop != nullptr);
    for (uint32_t index = 0; index < 10; ++index) {
        ASSERT_EQ(expectVersions[index], meta.schemaversions(index));
        ASSERT_EQ(expectVersions[index], infop->getTopicMeta().schemaversions(index));
    }
}

TEST_F(SysControllerTest, testNormalTopicRegisterSchema) {
    _sysController->setPrimary(true);
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    topicCreateRequest.set_topicname("topica");
    topicCreateRequest.set_partitioncount(1);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());

    RegisterSchemaRequest request;
    RegisterSchemaResponse response;
    request.set_topicname("topica");
    // schema empty
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    request.set_schema("schema");
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    uint32_t hash = static_cast<int32_t>(std::hash<string>{}("schema"));
    EXPECT_EQ(hash, response.version());

    request.set_version(100);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(100, response.version());

    response.set_version(0);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(100, response.version());
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_ALREADY_EXIST, response.errorinfo().errcode());
    {
        RegisterSchemaResponse response;
        request.set_version(0);
        _sysController->registerSchema(&request, &response);
        ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());
        ASSERT_EQ("ERROR_ADMIN_INVALID_PARAMETER[version 0 is not allowed!]", response.errorinfo().errmsg());
        EXPECT_EQ(0, response.version());
    }
}

TEST_F(SysControllerTest, testRegisterSchemaSetVersion) {
    _sysController->setPrimary(true);
    // support schema
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    topicCreateRequest.set_topicname("topica");
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_needschema(true);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(ERROR_NONE, topicCreateResponse.errorinfo().errcode());

    // invalid schema
    RegisterSchemaRequest request;
    RegisterSchemaResponse response;
    request.set_version(10);
    request.set_topicname("topica");
    request.set_schema("schema");
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    string schemaStr(
        R"json({"topic":"topica","fields":[{"name":"CMD","type":"string"},{"name":"shop_id","type":"string"}]})json");
    request.set_schema(schemaStr);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(10, response.version());

    TopicCreationRequest meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->getTopicMeta("topica", meta));
    ASSERT_EQ(meta.topicname(), topicCreateRequest.topicname());
    ASSERT_EQ(meta.needschema(), topicCreateRequest.needschema());
    ASSERT_EQ(1, meta.schemaversions_size());
    ASSERT_EQ(10, meta.schemaversions(0));

    // version already exist
    response.set_version(0);
    _sysController->registerSchema(&request, &response);
    ASSERT_EQ(10, response.version());
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_ALREADY_EXIST, response.errorinfo().errcode());
}

void SysControllerTest::assertGetSchema(bool isFollower) {
    _sysController->setPrimary(isFollower);
    GetSchemaRequest request;
    GetSchemaResponse response;
    // no topic no version
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // no version
    request.set_topicname("topica");
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // no topic
    request.clear_topicname();
    request.set_version(100);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, response.errorinfo().errcode());

    // topic not exist
    request.set_topicname("topica");
    request.set_version(100);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());

    // normal topic not found schema
    TopicCreationRequest meta;
    meta.set_topicname("topica");
    meta.set_partitioncount(1);
    _sysController->_topicTable.addTopic(&meta);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, response.errorinfo().errcode());

    // schema not found
    TopicInfoPtr infop = _sysController->_topicTable.findTopic("topica");
    EXPECT_TRUE(infop != nullptr);
    infop->_topicMeta.set_needschema(true);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, response.errorinfo().errcode());

    // zk dataAccessor not found schema
    string schemaStr(
        R"json({"topic":"topica","fields":[{"name":"CMD","type":"string"},{"name":"shop_id","type":"string"}]})json");
    FieldSchema fschema;
    fschema.fromJsonString(schemaStr);
    uint32_t version = fschema.calcVersion();
    infop->_topicMeta.add_schemaversions(version);
    request.set_version(version);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_ADMIN_SCHEMA_NOT_FOUND, response.errorinfo().errcode());

    // found schema
    SchemaInfo si;
    si.set_version(version);
    si.set_schema(schemaStr);
    si.set_registertime(100);
    SchemaInfos scmInfos;
    *scmInfos.add_sinfos() = si;
    _sysController->_zkDataAccessor->_topicSchemas["topica"] = scmInfos;
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(version, response.schemainfo().version());
    EXPECT_EQ(schemaStr, response.schemainfo().schema());
    EXPECT_EQ(100, response.schemainfo().registertime());

    // get latest
    request.set_version(0);
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(version, response.schemainfo().version());
    EXPECT_EQ(schemaStr, response.schemainfo().schema());
    EXPECT_EQ(100, response.schemainfo().registertime());

    si.set_version(200);
    si.set_schema("xxx");
    si.set_registertime(200);
    *scmInfos.add_sinfos() = si;
    _sysController->_zkDataAccessor->_topicSchemas["topica"] = scmInfos;
    _sysController->getSchema(&request, &response);
    ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
    EXPECT_EQ(200, response.schemainfo().version());
    EXPECT_EQ("xxx", response.schemainfo().schema());
    EXPECT_EQ(200, response.schemainfo().registertime());
}

TEST_F(SysControllerTest, testGetSchemaLeader) { assertGetSchema(true); }

TEST_F(SysControllerTest, testGetSchemaFollower) { assertGetSchema(false); }

TEST_F(SysControllerTest, testCheckTopicExist) {
    SysController sysCtr(nullptr, nullptr);
    TopicCreationRequest meta;
    meta.set_topicname("topica");
    EXPECT_FALSE(sysCtr.checkTopicExist(&meta));
    sysCtr._topicTable.addTopic(&meta);
    EXPECT_TRUE(sysCtr.checkTopicExist(&meta));
}

TEST_F(SysControllerTest, testCheckSameTopic) {
    SysController sysCtr(nullptr, nullptr);
    {
        string sameTopic;
        TopicBatchCreationRequest request;
        EXPECT_TRUE(sysCtr.checkSameTopic(&request, sameTopic));
        TopicCreationRequest *meta = request.add_topicrequests();
        meta->set_topicname("aa");
        EXPECT_TRUE(sysCtr.checkSameTopic(&request, sameTopic));
        meta = request.add_topicrequests();
        meta->set_topicname("aa");
        EXPECT_FALSE(sysCtr.checkSameTopic(&request, sameTopic));
        EXPECT_EQ("aa", sameTopic);
        meta = request.add_topicrequests();
        meta->set_topicname("bb");
        EXPECT_FALSE(sysCtr.checkSameTopic(&request, sameTopic));
        EXPECT_EQ("aa", sameTopic);
    }
    {
        string sameTopic;
        TopicBatchCreationRequest request;
        TopicCreationRequest *meta = request.add_topicrequests();
        meta->set_topicname("aa");
        meta = request.add_topicrequests();
        meta->set_topicname("bb");
        EXPECT_TRUE(sysCtr.checkSameTopic(&request, sameTopic));
        meta = request.add_topicrequests();
        EXPECT_TRUE(sameTopic.empty());
        meta->set_topicname("aa");
        EXPECT_FALSE(sysCtr.checkSameTopic(&request, sameTopic));
        EXPECT_EQ("aa", sameTopic);
    }
}

TEST_F(SysControllerTest, testErrorCodeCompatibal) {
    TopicInfoResponse response;
    ErrorInfo *error = response.mutable_errorinfo();
    error->set_errcode(ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT);
    error->set_errmsg("ERROR_ADMIN_WAIT_IN_QUEUE_TIMEOUT");
    cout << "src: " << response.ShortDebugString() << endl;

    // write and read should different version
    ofstream ofs("/swift_develop/source_code/swift/response", std::ios_base::out | std::ios_base::binary);
    response.SerializeToOstream(&ofs);
    ofs.close();

    ifstream iss("/swift_develop/source_code/swift/response", std::ios_base::out | std::ios_base::binary);
    TopicInfoResponse result;
    result.ParseFromIstream(&iss);
    cout << "result: " << result.ShortDebugString() << endl;
    cout << "ec: " << result.errorinfo().errcode() << endl;
    cout << "msg: " << result.errorinfo().errmsg() << endl;
}

TEST_F(SysControllerTest, testUpdatePartitionResource) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    for (int32_t i = 1; i < 4; ++i) {
        string topicName = "topic_" + StringUtil::toString(i);
        topicCreateRequest.set_topicname(topicName);
        topicCreateRequest.set_partitioncount(i);
        topicCreateRequest.set_resource(i);
        _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
        ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    }
    TopicMap topicMap;
    _sysController->_topicTable.getTopicMap(topicMap);

    //_topicInStatusManager getParititionResource fail, not change
    _sysController->updatePartitionResource(topicMap);
    _sysController->_topicTable.getTopicMap(topicMap);
    EXPECT_EQ(1, topicMap["topic_1"]->getResource());
    EXPECT_EQ(2, topicMap["topic_2"]->getResource());
    EXPECT_EQ(3, topicMap["topic_3"]->getResource());

    // set _topicInStatusManager, not set config auto update resource
    TopicInStatus &tis2 = _sysController->_topicInStatusManager._topicStatusMap["topic_2"];
    PartitionInStatus pis;
    pis.resource = 200;
    tis2._rwMetrics.push_back(pis);
    TopicInStatus &tis3 = _sysController->_topicInStatusManager._topicStatusMap["topic_3"];
    pis.resource = 300;
    tis3._rwMetrics.push_back(pis);
    _sysController->updatePartitionResource(topicMap);
    _sysController->_topicTable.getTopicMap(topicMap);
    EXPECT_EQ(1, topicMap["topic_1"]->getResource());
    EXPECT_EQ(200, topicMap["topic_2"]->getResource());
    EXPECT_EQ(300, topicMap["topic_3"]->getResource());
}

TEST_F(SysControllerTest, testModifyTopicExtendDfsRoot) {
    _sysController->setPrimary(true);
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse response;
    string topicName("topic_edfs");
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(1);
    topicCreateRequest.set_dfsroot("hdfs://aaa");
    _sysController->createTopic(&topicCreateRequest, &response);
    ASSERT_EQ(protocol::ERROR_NONE, response.errorinfo().errcode());
    TopicInfoPtr tinfo = _sysController->_topicTable.findTopic(topicName);
    const TopicCreationRequest &meta = tinfo->getTopicMeta();
    ASSERT_EQ(topicName, meta.topicname());
    ASSERT_EQ(string("hdfs://aaa"), meta.dfsroot());
    ASSERT_EQ(0, meta.extenddfsroot_size());

    // set extendDfsRoot
    {
        TopicCreationRequest modifyRequest;
        modifyRequest.set_topicname(topicName);
        *modifyRequest.add_extenddfsroot() = "hdfs://bbb";
        _sysController->modifyTopic(&modifyRequest, &response);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
        ASSERT_EQ(string("hdfs://aaa"), meta.dfsroot());
        ASSERT_EQ(1, meta.extenddfsroot_size());
        ASSERT_EQ(string("hdfs://bbb"), meta.extenddfsroot(0));
    }

    // extendDfsRoot set empty means clear dfsroot
    {
        TopicCreationRequest modifyRequest;
        modifyRequest.set_topicname(topicName);
        *modifyRequest.add_extenddfsroot() = "";
        _sysController->modifyTopic(&modifyRequest, &response);
        ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
        ASSERT_EQ(string("hdfs://aaa"), meta.dfsroot());
        ASSERT_EQ(0, meta.extenddfsroot_size());
    }
}

TEST_F(SysControllerTest, testGetDeletedNoUseTopicAndFiles) {
    // 1. nothing
    EmptyRequest emptyRequest;
    DeletedNoUseTopicFilesResponse dntfResponse;
    _sysController->getDeletedNoUseTopicFiles(&emptyRequest, &dntfResponse);
    ASSERT_EQ(ERROR_NONE, dntfResponse.errorinfo().errcode());
    ASSERT_EQ(0, dntfResponse.filenames_size());

    DeletedNoUseTopicRequest dntRequest;
    DeletedNoUseTopicResponse dntResponse;
    _sysController->getDeletedNoUseTopic(&dntRequest, &dntResponse);
    ASSERT_EQ(ERROR_ADMIN_INVALID_PARAMETER, dntResponse.errorinfo().errcode());

    // 2. write zk
    TopicCreationRequest meta;
    TopicMetas writeMetas;
    meta.set_topicname("topic1");
    meta.set_partitioncount(1);
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeDeletedNoUseTopics(writeMetas));
    dntfResponse.clear_filenames();
    _sysController->getDeletedNoUseTopicFiles(&emptyRequest, &dntfResponse);
    ASSERT_EQ(ERROR_NONE, dntfResponse.errorinfo().errcode());
    ASSERT_EQ(1, dntfResponse.filenames_size());
    string firstZkFile = dntfResponse.filenames(0);
    dntRequest.set_filename(firstZkFile);
    dntResponse.clear_topicmetas();
    _sysController->getDeletedNoUseTopic(&dntRequest, &dntResponse);
    ASSERT_EQ(ERROR_NONE, dntResponse.errorinfo().errcode());
    ASSERT_EQ(1, dntResponse.topicmetas_size());
    auto resMeta = dntResponse.topicmetas(0);
    ASSERT_EQ("topic1", resMeta.topicname());
    ASSERT_EQ(1, resMeta.partitioncount());

    sleep(1);
    meta.set_topicname("topic2");
    meta.set_partitioncount(2);
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeDeletedNoUseTopics(writeMetas));
    dntfResponse.clear_filenames();
    _sysController->getDeletedNoUseTopicFiles(&emptyRequest, &dntfResponse);
    ASSERT_EQ(ERROR_NONE, dntfResponse.errorinfo().errcode());
    ASSERT_EQ(2, dntfResponse.filenames_size());
    {
        dntRequest.set_filename(firstZkFile);
        dntResponse.clear_topicmetas();
        _sysController->getDeletedNoUseTopic(&dntRequest, &dntResponse);
        ASSERT_EQ(ERROR_NONE, dntResponse.errorinfo().errcode());
        ASSERT_EQ(1, dntResponse.topicmetas_size());
        resMeta = dntResponse.topicmetas(0);
        ASSERT_EQ("topic1", resMeta.topicname());
        ASSERT_EQ(1, resMeta.partitioncount());

        string secondZkFile =
            (firstZkFile == dntfResponse.filenames(0)) ? dntfResponse.filenames(1) : dntfResponse.filenames(0);
        dntResponse.clear_topicmetas();
        dntRequest.set_filename(secondZkFile);
        _sysController->getDeletedNoUseTopic(&dntRequest, &dntResponse);
        ASSERT_EQ(ERROR_NONE, dntResponse.errorinfo().errcode());
        ASSERT_EQ(2, dntResponse.topicmetas_size());
        resMeta = dntResponse.topicmetas(0);
        ASSERT_EQ("topic1", resMeta.topicname());
        ASSERT_EQ(1, resMeta.partitioncount());
        resMeta = dntResponse.topicmetas(1);
        ASSERT_EQ("topic2", resMeta.topicname());
        ASSERT_EQ(2, resMeta.partitioncount());
    }
}

TEST_F(SysControllerTest, testDeleteNoUseTopic) {
    FileSystem::remove(_zkRoot + "nouse_topics/");
    TopicBatchCreationRequest topicCreateRequest;
    TopicBatchCreationResponse topicCreateResponse;
    TopicCreationRequest meta;
    uint32_t curTime = TimeUtility::currentTimeInSeconds();
    for (int i = 0; i < 5; ++i) {
        meta.set_topicname("topic_" + to_string(i));
        meta.set_partitioncount(i + 1);
        uint64_t topicTime = curTime - 100 * (5 - i);
        topicTime *= 1000000;
        meta.set_createtime(topicTime);
        meta.set_modifytime(topicTime);
        *topicCreateRequest.add_topicrequests() = meta;
    }
    _sysController->setPrimary(true);
    _sysController->_moduleManager->loadModule<NoUseTopicModule>();

    _sysController->createTopicBatch(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());
    ASSERT_EQ(5, _sysController->_topicTable._topicMap.size());

    // begin test
    // 1. not set config noUseTopicExpireTimeSec
    auto noUseTopicModule = _sysController->_moduleManager->getModule<NoUseTopicModule>();
    noUseTopicModule->deleteNoUseTopic();
    ASSERT_EQ(5, _sysController->_topicTable._topicMap.size());

    // 2. topic rw infos empty, not delete
    _sysController->_adminConfig->noUseTopicExpireTimeSec = 150;
    _sysController->_adminConfig->noUseTopicDeleteIntervalSec = 150;
    noUseTopicModule->deleteNoUseTopic();
    ASSERT_EQ(5, _sysController->_topicTable._topicMap.size());

    // set topic rw infos, assume topic_0 has no info, topic_4 only write
    // topic_3 only read, topic_2, topic_1 both
    TopicRWInfos rwInfos;
    for (int i = 1; i < 5; ++i) {
        TopicRWInfo *info = rwInfos.add_infos();
        info->set_topicname("topic_" + to_string(i));
        if (i != 3) {
            info->set_lastwritetime(curTime - 100 * i);
        }
        if (i != 4) {
            info->set_lastreadtime(curTime - 100 * i);
        }
    }
    _sysController->_topicInStatusManager.updateTopicWriteReadTime(rwInfos);

    // 3. last delete already, will not delete
    const string &lastDelFileMock = "/nouse_topics/" + to_string(curTime - 100);
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeFile(lastDelFileMock, "aaa"));
    noUseTopicModule->deleteNoUseTopic();
    ASSERT_EQ(5, _sysController->_topicTable._topicMap.size());

    // 4. normal delete
    const string &newDelFileMock = "/nouse_topics/" + to_string(curTime - 151);
    ASSERT_EQ(fslib::EC_OK, FileSystem::rename(_zkRoot + lastDelFileMock, _zkRoot + newDelFileMock));
    noUseTopicModule->deleteNoUseTopic();
    TopicMap topicMap;
    _sysController->_topicTable.getTopicMap(topicMap);
    ASSERT_EQ(3, topicMap.size());
    ASSERT_TRUE(topicMap.find("topic_0") != topicMap.end());
    ASSERT_TRUE(topicMap.find("topic_1") != topicMap.end());
    ASSERT_TRUE(topicMap.find("topic_4") != topicMap.end());
}

TEST_F(SysControllerTest, testLoadLatestDeletedNoUseTopics) {
    FileSystem::remove(_zkRoot + "nouse_topics/");
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(_zkRoot + "nouse_topics/"));

    auto noUseTopicModule = _sysController->_moduleManager->getModule<NoUseTopicModule>();

    // 1. no use dir empty
    ASSERT_TRUE(noUseTopicModule->loadLastDeletedNoUseTopics());
    ASSERT_EQ(0, noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.size());

    // 2. file illegal
    uint32_t curTime = TimeUtility::currentTimeInSeconds();
    string lastDelFileMock = "/nouse_topics/" + to_string(curTime - 200);
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeFile(lastDelFileMock, "aaa"));
    ASSERT_FALSE(noUseTopicModule->loadLastDeletedNoUseTopics());
    ASSERT_EQ(0, noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.size());

    // 3. normal
    lastDelFileMock = "/nouse_topics/" + to_string(curTime - 100);
    TopicCreationRequest meta;
    TopicMetas writeMetas;
    meta.set_topicname("topic1");
    meta.set_partitioncount(1);
    *writeMetas.add_topicmetas() = meta;
    meta.set_topicname("topic2");
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeZk(lastDelFileMock, writeMetas, true));
    ASSERT_TRUE(noUseTopicModule->loadLastDeletedNoUseTopics());
    ASSERT_EQ(2, noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.size());
    ASSERT_TRUE(noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.end() !=
                noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.find("topic1"));
    ASSERT_TRUE(noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.end() !=
                noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.find("topic2"));

    // 4. more
    lastDelFileMock = "/nouse_topics/" + to_string(curTime);
    writeMetas.Clear();
    meta.set_topicname("topic3");
    *writeMetas.add_topicmetas() = meta;
    ASSERT_TRUE(_sysController->_zkDataAccessor->writeZk(lastDelFileMock, writeMetas, true));
    ASSERT_TRUE(noUseTopicModule->loadLastDeletedNoUseTopics());
    ASSERT_EQ(1, noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.size());
    ASSERT_TRUE(noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.end() !=
                noUseTopicModule->_noUseTopicInfo._lastNoUseTopics.find("topic3"));
}

TEST_F(SysControllerTest, testInit) {
    {
        AdminConfig config;
        SysController ctrl(&config, nullptr);
        ASSERT_TRUE(ctrl.init());
        ASSERT_FALSE(ctrl._adminConfig->isLocalMode());
        WorkerManager *wm = ctrl._workerManager.get();
        ASSERT_EQ(string(typeid(WorkerManager).name()), string(typeid(*wm).name()));
    }
    {
        AdminConfig config;
        SysController ctrl(&config, nullptr);
        config.localMode = true;
        ASSERT_TRUE(ctrl.init());
        ASSERT_TRUE(ctrl._adminConfig->isLocalMode());
        WorkerManager *wm = ctrl._workerManager.get();
        ASSERT_EQ(string(typeid(WorkerManagerLocal).name()), string(typeid(*wm).name()));
    }
}

TEST_F(SysControllerTest, testFillTaskInfo) {
    TaskInfo ti;
    TopicCreationRequest meta;
    meta.set_partitionminbuffersize(9999);
    meta.set_partitionmaxbuffersize(9999);
    meta.set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    meta.set_needfieldfilter(false);
    meta.set_obsoletefiletimeinterval(1);
    meta.set_reservedfilecount(2);
    meta.set_maxwaittimeforsecuritycommit(123);
    meta.set_maxdatasizeforsecuritycommit(456);
    meta.set_compressmsg(true);
    meta.set_compressthres(100);
    meta.set_dfsroot("hdfs://xxx");
    *meta.add_extenddfsroot() = "hdfs://xxx/extend";
    *meta.add_extenddfsroot() = "hdfs://xxx";
    ASSERT_FALSE(meta.has_readnotcommittedmsg());

    SysController ctrl(nullptr, nullptr);
    ctrl.fillTaskInfo(&ti, meta);
    ASSERT_EQ(9999, ti.minbuffersize());
    ASSERT_EQ(9999, ti.maxbuffersize());
    ASSERT_EQ(TOPIC_MODE_MEMORY_ONLY, ti.topicmode());
    ASSERT_EQ(false, ti.needfieldfilter());
    ASSERT_EQ(1, ti.obsoletefiletimeinterval());
    ASSERT_EQ(2, ti.reservedfilecount());
    ASSERT_EQ(123, ti.maxwaittimeforsecuritycommit());
    ASSERT_EQ(456, ti.maxdatasizeforsecuritycommit());
    ASSERT_EQ(true, ti.compressmsg());
    // ASSERT_EQ(100, ti.compressthres()); // BUG: wrong compressthres type
    ASSERT_EQ(false, ti.sealed());
    ASSERT_EQ("hdfs://xxx", ti.dfsroot());
    ASSERT_EQ(2, ti.extenddfsroot_size());
    ASSERT_TRUE(ti.enablettldel());
    ASSERT_EQ(-1, ti.readsizelimitsec());
    ASSERT_EQ(false, ti.enablelongpolling());
    ASSERT_FALSE(ti.versioncontrol());
    ASSERT_FALSE(ti.has_readnotcommittedmsg());
    ASSERT_TRUE(ti.readnotcommittedmsg());

    // change topic info
    meta.set_partitionminbuffersize(1234);
    meta.set_partitionmaxbuffersize(5678);
    meta.set_needfieldfilter(true);
    meta.set_sealed(true);
    meta.set_enablettldel(false);
    meta.set_readsizelimitsec(200);
    meta.set_enablelongpolling(true);
    meta.set_versioncontrol(true);
    meta.set_readnotcommittedmsg(false);

    ASSERT_TRUE(meta.has_readnotcommittedmsg());
    ctrl.fillTaskInfo(&ti, meta);
    ASSERT_EQ(1234, ti.minbuffersize());
    ASSERT_EQ(5678, ti.maxbuffersize());
    ASSERT_EQ(true, ti.needfieldfilter());
    ASSERT_EQ(true, ti.sealed());
    ASSERT_FALSE(ti.enablettldel());
    ASSERT_EQ(200, ti.readsizelimitsec());
    ASSERT_TRUE(ti.enablelongpolling());
    ASSERT_TRUE(ti.versioncontrol());
    ASSERT_FALSE(ti.readnotcommittedmsg());
}

TEST_F(SysControllerTest, testCanSealedTopicModify) {
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        meta.set_partitioncount(12);

        request.set_sealed(true);
        request.set_partitioncount(10);
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(false);
        meta.set_partitioncount(12);

        request.set_sealed(false);
        request.set_partitioncount(10);
        SysController ctrl(nullptr, nullptr);
        ASSERT_TRUE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);

        request.set_sealed(false);
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        meta.set_topicmode(TOPIC_MODE_NORMAL);
        request.set_sealed(true);
        request.set_topicmode(TOPIC_MODE_SECURITY);
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        meta.set_needschema(false);
        request.set_sealed(true);
        request.set_needschema(true);
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        request.set_sealed(true);
        request.set_topictype(TOPIC_TYPE_NORMAL);
        SysController ctrl(nullptr, nullptr);
        ASSERT_TRUE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        request.set_sealed(true);
        request.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        *(meta.add_extenddfsroot()) = "extdfsroot1";
        *(meta.add_extenddfsroot()) = "extdfsroot2";

        request.set_sealed(true);
        *(request.add_extenddfsroot()) = "extdfsroot2";
        *(request.add_extenddfsroot()) = "extdfsroot1";

        SysController ctrl(nullptr, nullptr);
        ASSERT_TRUE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        *(meta.add_extenddfsroot()) = "extdfsroot1";
        *(meta.add_extenddfsroot()) = "extdfsroot2";

        request.set_sealed(true);
        *(request.add_extenddfsroot()) = "extdfsroot2";

        SysController ctrl(nullptr, nullptr);
        ASSERT_TRUE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        *(meta.add_extenddfsroot()) = "extdfsroot2";

        request.set_sealed(true);
        *(request.add_extenddfsroot()) = "extdfsroot1";
        *(request.add_extenddfsroot()) = "extdfsroot2";

        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        *(meta.add_extenddfsroot()) = "extdfsroot2";

        request.set_sealed(true);
        *(request.add_extenddfsroot()) = "extdfsroot1";
        SysController ctrl(nullptr, nullptr);
        ASSERT_FALSE(ctrl.canSealedTopicModify(meta, &request));
    }
    {
        TopicCreationRequest meta;
        TopicCreationRequest request;
        meta.set_sealed(true);
        *(meta.add_extenddfsroot()) = "extdfsroot2";

        request.set_sealed(true);
        SysController ctrl(nullptr, nullptr);
        ASSERT_TRUE(ctrl.canSealedTopicModify(meta, &request));
    }
}

TEST_F(SysControllerTest, testChangeTopicPartCnt) {
    // AUTIL_ROOT_LOG_SETLEVEL(INFO);
    _sysController->setPrimary(true);
    TopicCreationRequest meta;
    TopicBatchCreationRequest createRequest;
    TopicBatchCreationResponse createResponse;
    ChangeTopicPartCntTask task;
    uint32_t curTime = TimeUtility::currentTime();

    // create topics
    meta.set_topicname("topic_pl1");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.clear_physictopiclst();
    meta.set_sealed(false);
    meta.set_partitioncount(6);
    meta.set_modifytime(100);
    *createRequest.add_topicrequests() = meta;

    meta.set_topicname("topic_pl2");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.clear_physictopiclst();
    *meta.add_physictopiclst() = "topic_pl2-111-6";
    meta.set_sealed(true);
    meta.set_partitioncount(6);
    meta.set_modifytime(200);
    *createRequest.add_topicrequests() = meta;

    _sysController->createTopicBatch(&createRequest, &createResponse);
    ASSERT_EQ(ERROR_NONE, createResponse.errorinfo().errcode());
    auto &da = _sysController->_zkDataAccessor;

    // add change partition count tasks
    meta.set_topicname("topic_pl1");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.set_modifytime(101);
    meta.clear_physictopiclst();
    meta.set_partitioncount(8);
    meta.set_sealed(false);
    da->sendChangePartCntTask(meta);
    ASSERT_EQ(1, da->getChangePartCntTasks().tasks_size());

    meta.set_topicname("topic_pl2");
    meta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    meta.set_modifytime(202);
    meta.clear_physictopiclst();
    *meta.add_physictopiclst() = "topic_pl2-111-6";
    meta.set_sealed(true);
    meta.set_partitioncount(4);
    da->sendChangePartCntTask(meta);
    ASSERT_EQ(2, da->getChangePartCntTasks().tasks_size());

    // start change partition count task
    TopicInfoPtr topicInfo;
    topicInfo = _sysController->_topicTable.findTopic("topic_pl1");
    ASSERT_EQ(false, topicInfo->_topicMeta.sealed());
    topicInfo = _sysController->_topicTable.findTopic("topic_pl2");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
    topicInfo = _sysController->_topicTable.findTopic("topic_pl2-111-6");
    ASSERT_EQ(false, topicInfo->_topicMeta.sealed());

    ASSERT_EQ(2, da->getChangePartCntTasks().tasks_size());
    ASSERT_EQ(101, da->getChangePartCntTasks().tasks(0).taskid());
    ASSERT_EQ(202, da->getChangePartCntTasks().tasks(1).taskid());

    _sysController->changeTopicPartCnt();
    ASSERT_EQ(2, da->getChangePartCntTasks().tasks_size());
    ASSERT_EQ(101, da->getChangePartCntTasks().tasks(0).taskid());
    ASSERT_EQ(202, da->getChangePartCntTasks().tasks(1).taskid());

    topicInfo = _sysController->_topicTable.findTopic("topic_pl1");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
    ASSERT_EQ(0, topicInfo->physicTopicLst().size());
    topicInfo = _sysController->_topicTable.findTopic("topic_pl2");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
    ASSERT_EQ(1, topicInfo->physicTopicLst().size());
    topicInfo = _sysController->_topicTable.findTopic("topic_pl2-111-6");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());

    RoleInfo roleInfo("role_1", "127.0.0.1", 1111);
    WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
    HeartbeatInfo current;
    current.set_alive(true);
    current.set_address("role_1#_#127.0.0.1:1111");
    TaskStatus taskStatus;
    TaskInfo taskInfo;
    PartitionId partitionId;
    partitionId.set_topicname("topic_pl2-111-6");
    taskInfo.set_sealed(true);
    for (int i = 0; i < 6; ++i) {
        partitionId.set_id(i);
        *taskInfo.mutable_partitionid() = partitionId;
        *taskStatus.mutable_taskinfo() = taskInfo;
        *current.add_tasks() = taskStatus;
    }

    workerInfoPtr->setCurrent(current);
    _sysController->_workerTable._brokerWorkerMap["role_1"] = workerInfoPtr;

    _sysController->changeTopicPartCnt();
    ASSERT_EQ(1, da->getChangePartCntTasks().tasks_size());
    ASSERT_EQ(101, da->getChangePartCntTasks().tasks(0).taskid());

    topicInfo = _sysController->_topicTable.findTopic("topic_pl1");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
    ASSERT_EQ(0, topicInfo->physicTopicLst().size());
    topicInfo = _sysController->_topicTable.findTopic("topic_pl2");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
    ASSERT_EQ(2, topicInfo->physicTopicLst().size());
    const auto &physicNameLst = topicInfo->physicTopicLst();
    topicInfo = _sysController->_topicTable.findTopic(physicNameLst[0]);
    ASSERT_TRUE(topicInfo->_topicMeta.sealed());
    topicInfo = _sysController->_topicTable.findTopic(physicNameLst[1]);
    ASSERT_FALSE(topicInfo->_topicMeta.sealed());

    string logicName;
    int64_t timestamp;
    uint32_t partCnt;
    ASSERT_TRUE(LogicTopicHelper::parsePhysicTopicName(physicNameLst[1], logicName, timestamp, partCnt));
    ASSERT_EQ("topic_pl2", logicName);
    ASSERT_TRUE(timestamp >= curTime);
    ASSERT_TRUE(timestamp <= TimeUtility::currentTime());
    ASSERT_EQ(4, partCnt);

    topicInfo = _sysController->_topicTable.findTopic("topic_pl2-111-6");
    ASSERT_EQ(true, topicInfo->_topicMeta.sealed());
}

TEST_F(SysControllerTest, testUpdateSealTopicStatus) {
    _sysController->setPrimary(true);
    unordered_set<string> sealedTopicSet;

    TopicCreationRequest request;
    for (int i = 1; i <= 3; ++i) {
        request.set_topicname("topic_" + to_string(i));
        request.set_partitioncount(i);
        _sysController->_topicTable.addTopic(&request);
    }
    for (int brokerId = 0; brokerId < 2; ++brokerId) {
        string roleName = "broker_" + to_string(brokerId);
        string address = roleName + "#_#127.0.0.1:0";
        RoleInfo roleInfo(roleName, "127.0.0.1", 0);
        WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
        HeartbeatInfo current;
        current.set_address(address);
        current.set_alive(true);
        for (int topicId = brokerId; topicId < 3; ++topicId) { // topics
            TaskStatus taskStatus;
            TaskInfo taskInfo;
            PartitionId partitionId;
            partitionId.set_topicname("topic_" + to_string(topicId + 1));
            partitionId.set_id(brokerId);
            taskInfo.set_sealed(true);
            *taskInfo.mutable_partitionid() = partitionId;
            *taskStatus.mutable_taskinfo() = taskInfo;
            *current.add_tasks() = taskStatus;
        }
        workerInfoPtr->setCurrent(current);
        _sysController->_workerTable._brokerWorkerMap[roleName] = workerInfoPtr;
    }
    _sysController->updateSealTopicStatus(sealedTopicSet);
    EXPECT_EQ(1, sealedTopicSet.count("topic_1"));
    EXPECT_EQ(1, sealedTopicSet.count("topic_2"));
    EXPECT_EQ(0, sealedTopicSet.count("topic_3"));
}

TEST_F(SysControllerTest, testUpdateLoadedTopic) {
    SysController sysController(nullptr, nullptr);
    {
        string roleName = "broker_0_0";
        string address = roleName + "#_#127.0.0.1:0";
        RoleInfo roleInfo(roleName, "127.0.0.1", 0);
        WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
        HeartbeatInfo heartbeat;
        heartbeat.set_address(address);
        TaskStatus *taskStatus = heartbeat.add_tasks();
        TaskInfo *taskInfo = taskStatus->mutable_taskinfo();
        PartitionId partId;
        partId.set_topicname("cad_topic1");
        partId.set_id(0);
        *(taskInfo->mutable_partitionid()) = partId;
        workerInfoPtr->setCurrent(heartbeat);
        sysController._workerTable._brokerWorkerMap[roleName] = workerInfoPtr;
    }
    {
        string roleName = "broker_1_0";
        string address = roleName + "#_#127.0.0.1:123";
        RoleInfo roleInfo(roleName, "127.0.0.1", 123);
        WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
        HeartbeatInfo heartbeat;
        heartbeat.set_address(address);
        TaskStatus *taskStatus = heartbeat.add_tasks();
        TaskInfo *taskInfo = taskStatus->mutable_taskinfo();
        PartitionId partId;
        partId.set_topicname("cad_topic2");
        partId.set_id(0);
        *(taskInfo->mutable_partitionid()) = partId;
        TaskStatus *taskStatus2 = heartbeat.add_tasks();
        TaskInfo *taskInfo2 = taskStatus2->mutable_taskinfo();
        PartitionId partId2;
        partId2.set_topicname("cad_topic1");
        partId2.set_id(1);
        *(taskInfo2->mutable_partitionid()) = partId2;
        workerInfoPtr->setCurrent(heartbeat);
        sysController._workerTable._brokerWorkerMap[roleName] = workerInfoPtr;
    }
    unordered_set<string> topicSet;
    sysController.updateLoadedTopic(topicSet);
    EXPECT_EQ(2, topicSet.size());
    EXPECT_EQ(1, topicSet.count("cad_topic1"));
    EXPECT_EQ(1, topicSet.count("cad_topic2"));
}

TEST_F(SysControllerTest, testGetPhysicMetaFromLogic) {
    { // invalid physic list
        TopicCreationRequest logicMeta;
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        logicMeta.add_physictopiclst("logicTopic-invalid-name");
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 0, physicMeta);
            EXPECT_EQ(ERROR_ADMIN_INVALID_PARAMETER, ec);
        }
    }
    { // LP with 1 physic
        TopicCreationRequest logicMeta;
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 0, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-123000000-4", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(4, physicMeta.partitioncount());
            EXPECT_EQ(-1, physicMeta.reservedfilecount());
            EXPECT_EQ(false, physicMeta.sealed());
            EXPECT_EQ(false, physicMeta.enablettldel());
            EXPECT_EQ(-1, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
        }
    }
    { // L with 2 physic (not set topicexpiredtime)
        TopicCreationRequest logicMeta;
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        logicMeta.set_partitioncount(3);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        logicMeta.add_physictopiclst("logicTopic-456000000-2");
        logicMeta.set_dfsroot("dfs://xxx");
        logicMeta.set_modifytime(54321);
        logicMeta.set_reservedfilecount(2);
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 0, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-123000000-4", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(4, physicMeta.partitioncount());
            EXPECT_EQ(2, physicMeta.reservedfilecount());
            EXPECT_EQ(true, physicMeta.sealed());
            EXPECT_EQ(true, physicMeta.enablettldel());
            EXPECT_EQ(-1, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
            EXPECT_EQ("dfs://xxx", physicMeta.dfsroot());
            EXPECT_EQ(54321, physicMeta.modifytime());
        }
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 1, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-456000000-2", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(2, physicMeta.partitioncount());
            EXPECT_EQ(2, physicMeta.reservedfilecount());
            EXPECT_EQ(false, physicMeta.sealed());
            EXPECT_EQ(false, physicMeta.enablettldel());
            EXPECT_EQ(-1, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
            EXPECT_EQ("dfs://xxx", physicMeta.dfsroot());
            EXPECT_EQ(54321, physicMeta.modifytime());
        }
    }
    { // L with 1 physic
        TopicCreationRequest logicMeta;
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        logicMeta.set_partitioncount(3);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        logicMeta.set_dfsroot("dfs://xxx");
        logicMeta.set_modifytime(54321);
        logicMeta.set_reservedfilecount(2);
        logicMeta.set_obsoletefiletimeinterval(2);
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 0, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-123000000-4", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(4, physicMeta.partitioncount());
            EXPECT_EQ(2, physicMeta.reservedfilecount());
            EXPECT_EQ(false, physicMeta.sealed());
            EXPECT_EQ(true, physicMeta.enablettldel());
            EXPECT_EQ(-1, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
            EXPECT_EQ("dfs://xxx", physicMeta.dfsroot());
            EXPECT_EQ(54321, physicMeta.modifytime());
        }
    }
    { // L with 2 physic
        TopicCreationRequest logicMeta;
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        logicMeta.set_partitioncount(3);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        logicMeta.add_physictopiclst("logicTopic-456000000-2");
        logicMeta.set_dfsroot("dfs://xxx");
        logicMeta.set_modifytime(54321);
        logicMeta.set_reservedfilecount(2);
        logicMeta.set_obsoletefiletimeinterval(2);
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 0, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-123000000-4", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(4, physicMeta.partitioncount());
            EXPECT_EQ(2, physicMeta.reservedfilecount());
            EXPECT_EQ(true, physicMeta.sealed());
            EXPECT_EQ(true, physicMeta.enablettldel());
            EXPECT_EQ(456 + 2 * 3600, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
            EXPECT_EQ("dfs://xxx", physicMeta.dfsroot());
            EXPECT_EQ(54321, physicMeta.modifytime());
        }
        {
            TopicCreationRequest physicMeta;
            protocol::ErrorCode ec = _sysController->getPhysicMetaFromLogic(logicMeta, 1, physicMeta);
            EXPECT_EQ(ERROR_NONE, ec);
            EXPECT_EQ("logicTopic-456000000-2", physicMeta.topicname());
            EXPECT_EQ(TOPIC_TYPE_PHYSIC, physicMeta.topictype());
            EXPECT_EQ(2, physicMeta.partitioncount());
            EXPECT_EQ(2, physicMeta.reservedfilecount());
            EXPECT_EQ(false, physicMeta.sealed());
            EXPECT_EQ(false, physicMeta.enablettldel());
            EXPECT_EQ(-1, physicMeta.topicexpiredtime());
            EXPECT_EQ(0, physicMeta.physictopiclst_size());
            EXPECT_EQ("dfs://xxx", physicMeta.dfsroot());
            EXPECT_EQ(54321, physicMeta.modifytime());
        }
    }
}

TEST_F(SysControllerTest, testAddPhysicMetasForLogicTopic) {
    { // 1LP-1P
        TopicBatchCreationRequest topicMetas;
        TopicBatchCreationResponse response;
        TopicCreationRequest &logicMeta = *topicMetas.add_topicrequests();
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        EXPECT_EQ(1, topicMetas.topicrequests_size());
        protocol::ErrorCode ec = _sysController->addPhysicMetasForLogicTopic(topicMetas, &response);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(2, topicMetas.topicrequests_size());
    }
    { // 1N & 1L-2P
        TopicBatchCreationRequest topicMetas;
        TopicBatchCreationResponse response;
        TopicCreationRequest &normalMeta = *topicMetas.add_topicrequests();
        normalMeta.set_topicname("normalTopic");
        normalMeta.set_topictype(TOPIC_TYPE_NORMAL);
        TopicCreationRequest &logicMeta = *topicMetas.add_topicrequests();
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        logicMeta.add_physictopiclst("logicTopic-456000000-2");
        EXPECT_EQ(2, topicMetas.topicrequests_size());
        protocol::ErrorCode ec = _sysController->addPhysicMetasForLogicTopic(topicMetas, &response);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(4, topicMetas.topicrequests_size());
    }
    { // 1N & 1LP & 1L-1P
        TopicBatchCreationRequest topicMetas;
        TopicBatchCreationResponse response;
        TopicCreationRequest &normalMeta = *topicMetas.add_topicrequests();
        normalMeta.set_topicname("normalTopic");
        normalMeta.set_topictype(TOPIC_TYPE_NORMAL);
        TopicCreationRequest &logicPhysicMeta = *topicMetas.add_topicrequests();
        logicPhysicMeta.set_topicname("logicPhysicTopic");
        logicPhysicMeta.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
        TopicCreationRequest &logicMeta = *topicMetas.add_topicrequests();
        logicMeta.set_topicname("logicTopic");
        logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
        logicMeta.add_physictopiclst("logicTopic-123000000-4");
        EXPECT_EQ(3, topicMetas.topicrequests_size());
        protocol::ErrorCode ec = _sysController->addPhysicMetasForLogicTopic(topicMetas, &response);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ(4, topicMetas.topicrequests_size());
    }
}

TEST_F(SysControllerTest, testAdjustPartitionLimit) {
    _adminConfig.groupBrokerCountMap.clear();
    _adminConfig.groupBrokerCountMap["mainse"] = 3;
    _adminConfig.groupBrokerCountMap["default"] = 2;
    WorkerMap aliveBrokerWorkers;
    WorkerInfoPtr wi(new WorkerInfo(RoleInfo()));
    aliveBrokerWorkers["mainse##broker_1_0"] = wi;
    aliveBrokerWorkers["mainse##broker_2_0"] = wi;
    aliveBrokerWorkers["default##broker_1_0"] = wi;
    aliveBrokerWorkers["default##broker_2_0"] = wi;
    aliveBrokerWorkers["default##broker_3_0"] = wi;
    aliveBrokerWorkers["drc##broker_1_0"] = wi;

    TopicMap topicMap;
    TopicCreationRequest meta;
    TopicInfoPtr ti;

    meta.set_topicname("tmall");
    meta.set_partitioncount(3);
    meta.set_topicgroup("mainse");
    ti.reset(new TopicInfo(meta));
    topicMap["tmall"] = ti;

    meta.set_topicname("inshop");
    meta.set_partitioncount(4);
    meta.set_topicgroup("mainse");
    meta.set_partitionlimit(1);
    ti.reset(new TopicInfo(meta));
    topicMap["inshop"] = ti;

    meta.set_topicname("order_buyer");
    meta.set_partitioncount(6);
    meta.set_topicgroup("default");
    meta.set_partitionlimit(10);
    ti.reset(new TopicInfo(meta));
    topicMap["order_buyer"] = ti;

    meta.set_topicname("no_group");
    meta.set_partitioncount(6);
    meta.set_topicgroup("no_group");
    meta.set_partitionlimit(10);
    ti.reset(new TopicInfo(meta));
    topicMap["no_group"] = ti;

    EXPECT_EQ(4, topicMap.size());
    _sysController->adjustPartitionLimit(topicMap, aliveBrokerWorkers);
    EXPECT_EQ(2, topicMap["tmall"]->getPartitionLimit());
    EXPECT_EQ(2, topicMap["inshop"]->getPartitionLimit());
    EXPECT_EQ(3, topicMap["order_buyer"]->getPartitionLimit());
    EXPECT_EQ(10, topicMap["no_group"]->getPartitionLimit());
}
TEST_F(SysControllerTest, testAdjustPartitionLimitWithDecsionThres) {
    _adminConfig.groupBrokerCountMap.clear();
    _adminConfig.groupBrokerCountMap["mainse"] = 10;
    WorkerMap aliveBrokerWorkers;
    WorkerInfoPtr wi(new WorkerInfo(RoleInfo()));
    aliveBrokerWorkers["mainse##broker_1_0"] = wi;
    aliveBrokerWorkers["mainse##broker_2_0"] = wi;
    TopicMap topicMap;
    TopicCreationRequest meta;
    TopicInfoPtr ti;
    meta.set_topicname("tmall");
    meta.set_partitioncount(30);
    meta.set_partitionlimit(20);
    meta.set_topicgroup("mainse");
    ti.reset(new TopicInfo(meta));
    topicMap["tmall"] = ti;
    {
        _adminConfig.decsionThreshold = 0.3;
        _sysController->adjustPartitionLimit(topicMap, aliveBrokerWorkers);
        EXPECT_EQ(20, topicMap["tmall"]->getPartitionLimit());
    }

    {
        _adminConfig.decsionThreshold = 0.2;
        _sysController->adjustPartitionLimit(topicMap, aliveBrokerWorkers);
        EXPECT_EQ(15, topicMap["tmall"]->getPartitionLimit());
    }
}

TEST_F(SysControllerTest, testFilterWorkers) {
    WorkerMap workerMap;
    RoleInfo ri;
    // add alive worker
    WorkerInfoPtr wi1(new WorkerInfo(ri));
    wi1->_workerStatus = ROLE_DS_ALIVE;
    workerMap["role_1"] = wi1;
    // add zombie worker
    WorkerInfoPtr wi2(new WorkerInfo(ri));
    wi2->_workerStatus = ROLE_DS_ALIVE;
    wi2->_startTimeSec = 0;
    workerMap["role_2"] = wi2;
    // add dead worker
    WorkerInfoPtr wi3(new WorkerInfo(ri));
    wi3->_workerStatus = ROLE_DS_DEAD;
    workerMap["role_3"] = wi3;

    _adminConfig.forceScheduleTimeSecond = 10;
    WorkerMap wm = _sysController->filterWorkers(workerMap, true);
    EXPECT_EQ(1, wm.size());
    EXPECT_TRUE(wm.end() != wm.find("role_1"));
    wm = _sysController->filterWorkers(workerMap, false);
    EXPECT_EQ(1, wm.size());
    EXPECT_TRUE(wm.end() != wm.find("role_3"));
}

TEST_F(SysControllerTest, testAdjustTopicParams) {
    TopicCreationRequest request;
    TopicCreationRequest retMeta;
    request.set_topicname("test_mainse_topic");
    request.set_partitioncount(1);
    request.set_rangecountinpartition(100);
    request.set_partitionbuffersize(0);
    vector<string> owners;
    owners.emplace_back("linson");
    owners.emplace_back("dongyu");
    _adminConfig.topicGroupVec.emplace_back(make_pair("mainse", "group1"));
    _adminConfig.topicOwnerVec.emplace_back(make_pair("mainse", owners));
    EXPECT_EQ(ERROR_NONE, _sysController->adjustTopicParams(&request, retMeta));
    EXPECT_EQ("test_mainse_topic", retMeta.topicname());
    EXPECT_EQ(1, retMeta.partitioncount());
    EXPECT_EQ(1, retMeta.rangecountinpartition());
    EXPECT_EQ(1, retMeta.partitionminbuffersize());
    EXPECT_EQ(256, retMeta.partitionmaxbuffersize());
    EXPECT_EQ("group1", retMeta.topicgroup());
    EXPECT_EQ(2, retMeta.owners_size());
    EXPECT_EQ("linson", retMeta.owners(0));
    EXPECT_EQ("dongyu", retMeta.owners(1));
}

TEST_F(SysControllerTest, testUpdateOneTopicStatus) {
    _adminConfig.fastRecover = true;
    TopicCreationRequest meta;
    meta.set_topicname("topic");
    meta.set_partitioncount(5);
    TopicInfo topicInfo(meta);
    topicInfo.setCurrentRoleAddress(0, "broker1#_#1.1.1.0"); // p0 not change, no version
    topicInfo.setTargetRoleAddress(0, "broker1#_#1.1.1.0");
    topicInfo.setStatus(0, PARTITION_STATUS_RUNNING);
    topicInfo.setTargetRoleAddress(1, "broker2#_#1.1.1.1"); // p1 change
    topicInfo.setStatus(1, PARTITION_STATUS_RUNNING);
    topicInfo.setTargetRoleAddress(2, "broker3#_#1.1.1.2");  // p2 change, version back
    topicInfo.setCurrentRoleAddress(3, "broker1#_#1.1.1.3"); // p3 not change, has version
    topicInfo.setTargetRoleAddress(3, "broker1#_#1.1.1.3");
    topicInfo.setCurrentRoleAddress(4, "broker1#_#1.1.1.4"); // p4 change, no broker
    topicInfo.setTargetRoleAddress(4, "");
    topicInfo.setStatus(4, PARTITION_STATUS_RUNNING);
    uint64_t currentTime = TimeUtility::currentTime();
    uint64_t masterVersion = 1;
    util::InlineVersion curVersion(masterVersion, currentTime);
    util::InlineVersion futureVersion(masterVersion, currentTime + 10 * 1000 * 1000);
    topicInfo.setInlineVersion(2, futureVersion.toProto());
    topicInfo.setInlineVersion(3, curVersion.toProto());

    TopicPartitionInfo topicPartInfo;
    topicPartInfo.set_topicname("topic");
    PartitionInfo pinfo;
    pinfo.set_id(0);
    *topicPartInfo.add_partitioninfos() = pinfo;
    pinfo.set_id(1);
    *topicPartInfo.add_partitioninfos() = pinfo;
    pinfo.set_id(2);
    *topicPartInfo.add_partitioninfos() = pinfo;
    pinfo.set_id(3);
    *topicPartInfo.add_partitioninfos() = pinfo;
    pinfo.set_id(4);
    *topicPartInfo.add_partitioninfos() = pinfo;

    _sysController->_selfMasterVersion = masterVersion;
    EXPECT_TRUE(_sysController->updateOneTopicStatus(&topicInfo, &topicPartInfo));
    EXPECT_EQ(0, topicPartInfo.partitioninfos(0).id());
    EXPECT_EQ("broker1", topicPartInfo.partitioninfos(0).rolename());
    EXPECT_EQ("1.1.1.0", topicPartInfo.partitioninfos(0).brokeraddress());
    EXPECT_EQ(PARTITION_STATUS_RUNNING, topicInfo.getStatus(0));
    util::InlineVersion tpInlineVersion;
    tpInlineVersion.fromProto(topicInfo.getInlineVersion(0));
    EXPECT_TRUE(curVersion < tpInlineVersion);
    EXPECT_EQ(1, topicPartInfo.partitioninfos(1).id());
    EXPECT_EQ("broker2", topicPartInfo.partitioninfos(1).rolename());
    EXPECT_EQ("1.1.1.1", topicPartInfo.partitioninfos(1).brokeraddress());
    EXPECT_EQ(PARTITION_STATUS_RUNNING, topicInfo.getStatus(1));
    tpInlineVersion.fromProto(topicInfo.getInlineVersion(1));
    EXPECT_TRUE(curVersion < tpInlineVersion);
    EXPECT_EQ(2, topicPartInfo.partitioninfos(2).id());
    EXPECT_EQ("broker3", topicPartInfo.partitioninfos(2).rolename());
    EXPECT_EQ("1.1.1.2", topicPartInfo.partitioninfos(2).brokeraddress());
    EXPECT_EQ(PARTITION_STATUS_WAITING, topicInfo.getStatus(2));
    tpInlineVersion.fromProto(topicInfo.getInlineVersion(2));
    EXPECT_TRUE(futureVersion < tpInlineVersion);
    EXPECT_EQ(3, topicPartInfo.partitioninfos(3).id());
    EXPECT_TRUE(topicPartInfo.partitioninfos(3).rolename().empty());
    EXPECT_TRUE(topicPartInfo.partitioninfos(3).brokeraddress().empty());
    EXPECT_EQ(PARTITION_STATUS_WAITING, topicInfo.getStatus(3));
    tpInlineVersion.fromProto(topicInfo.getInlineVersion(2));
    EXPECT_TRUE(futureVersion < tpInlineVersion);
    EXPECT_EQ(4, topicPartInfo.partitioninfos(4).id());
    EXPECT_TRUE(topicPartInfo.partitioninfos(4).rolename().empty());
    EXPECT_TRUE(topicPartInfo.partitioninfos(4).brokeraddress().empty());
    EXPECT_EQ(PARTITION_STATUS_WAITING, topicInfo.getStatus(4));
}

TEST_F(SysControllerTest, testCanDeal) {
    AdminConfig configDefault;
    configDefault.errorBrokerDealRatio = 0.1;
    SysController sysCtr(&configDefault, nullptr);
    for (int i = 0; i < 5; ++i) {
        string roleName = "role_" + to_string(i);
        WorkerInfoPtr role(new WorkerInfo(RoleInfo("role", "127.1.1.0", 123)));
        sysCtr._workerTable._brokerWorkerMap[roleName] = role;
    }

    vector<string> workers;
    ASSERT_FALSE(sysCtr.canDeal(workers));
    workers.push_back("role_0");
    ASSERT_TRUE(sysCtr.canDeal(workers));
    workers.push_back("role_1");
    ASSERT_FALSE(sysCtr.canDeal(workers));

    for (int i = 5; i < 19; ++i) {
        string roleName = "role_" + to_string(i);
        WorkerInfoPtr role(new WorkerInfo(RoleInfo("role", "127.1.1.0", 123)));
        sysCtr._workerTable._brokerWorkerMap[roleName] = role;
    }
    ASSERT_FALSE(sysCtr.canDeal(workers));

    for (int i = 19; i < 20; ++i) {
        string roleName = "role_" + to_string(i);
        WorkerInfoPtr role(new WorkerInfo(RoleInfo("role", "127.1.1.0", 123)));
        sysCtr._workerTable._brokerWorkerMap[roleName] = role;
    }
    ASSERT_TRUE(sysCtr.canDeal(workers));
}

TEST_F(SysControllerTest, testDealErrorBrokers) {
    AdminConfig configDefault;
    configDefault.errorBrokerDealRatio = 0.1;
    configDefault.zfsTimeout = 2000;
    configDefault.commitDelayThresholdInSec = 3;
    MockSysController sysController(&configDefault, nullptr);
    for (int i = 0; i < 10; ++i) {
        string roleName = "role_" + to_string(i);
        WorkerInfoPtr role(new WorkerInfo(RoleInfo("role", "127.1.1.0", 123)));
        HeartbeatInfo hb;
        hb.set_address("default##" + roleName + "#_#127.2.2.2:1234");
        role->setCurrent(hb);
        sysController._workerTable._brokerWorkerMap[roleName] = role;
        BrokerInStatus status;
        status.updateTime = autil::TimeUtility::currentTimeInSeconds();
        status.zfsTimeout = 2000;
        status.commitDelay = 3000000;
        sysController._workerTable.addBrokerInMetric(roleName, status);
    }
    sysController._moduleManager = std::make_unique<ModuleManager>();
    sysController._zkDataAccessor = _sysController->_zkDataAccessor;
    sysController._moduleManager->init(&(sysController._adminStatusManager), &sysController, &configDefault);
    ASSERT_TRUE(sysController._moduleManager->loadModule<DealErrorBrokerModule>());
    auto dealErrorBrokerModule = sysController._moduleManager->getModule<DealErrorBrokerModule>();
    EXPECT_CALL(sysController, changeSlot(_, _)).Times(0);
    dealErrorBrokerModule->dealErrorBrokers();

    BrokerInStatus status;
    status.updateTime = autil::TimeUtility::currentTimeInSeconds();
    status.zfsTimeout = 2001;
    status.commitDelay = 3000000;
    HeartbeatInfo hb;
    hb.set_address("bad_role_name");
    sysController._workerTable._brokerWorkerMap["role_0"]->setCurrent(hb);
    sysController._workerTable.addBrokerInMetric("role_0", status);
    EXPECT_CALL(sysController, changeSlot(_, _)).Times(0);
    dealErrorBrokerModule->dealErrorBrokers();

    hb.set_address("default##role_0#_#127.2.2.2:1234");
    sysController._workerTable._brokerWorkerMap["role_0"]->setCurrent(hb);
    EXPECT_CALL(sysController, changeSlot(_, _)).Times(1);
    dealErrorBrokerModule->dealErrorBrokers();
}

TEST_F(SysControllerTest, testGetNotTimeoutCurrentPartition) {
    WorkerMap workerMap;
    TaskStatus task;

    RoleInfo ri;
    // add alive worker
    WorkerInfoPtr wi1(new WorkerInfo(ri));
    wi1->_workerStatus = ROLE_DS_ALIVE;
    TaskInfo *ti = task.mutable_taskinfo();
    ti->mutable_partitionid()->set_topicname("topic1");
    ti->mutable_partitionid()->set_id(1);
    *wi1->_snapshot.add_tasks() = task;
    workerMap["role_1"] = wi1;

    // add unkown worker
    WorkerInfoPtr wi2(new WorkerInfo(ri));
    wi2->_workerStatus = ROLE_DS_UNKOWN;
    wi2->_unknownStartTime = TimeUtility::currentTime() - 5 * 1000 * 1000;
    ti->mutable_partitionid()->set_topicname("topic2");
    ti->mutable_partitionid()->set_id(2);
    *wi2->_snapshot.add_tasks() = task;
    workerMap["role_2"] = wi2;

    // add zombie worker
    WorkerInfoPtr wi3(new WorkerInfo(ri));
    wi3->_workerStatus = ROLE_DS_ALIVE;
    wi3->_startTimeSec = TimeUtility::currentTimeInSeconds() - 10;
    ti->mutable_partitionid()->set_topicname("topic3");
    ti->mutable_partitionid()->set_id(3);
    *wi3->_snapshot.add_tasks() = task;
    workerMap["role_3"] = wi3;

    // add dead worker, will not filter
    WorkerInfoPtr wi4(new WorkerInfo(ri));
    wi4->_workerStatus = ROLE_DS_DEAD;
    ti->mutable_partitionid()->set_topicname("topic4");
    ti->mutable_partitionid()->set_id(4);
    *wi4->_snapshot.add_tasks() = task;
    workerMap["role_4"] = wi4;

    WorkerInfo::TaskSet tasks;
    _adminConfig.deadBrokerTimeoutSec = 13; // 13s
    tasks = _sysController->getNotTimeoutCurrentPartition(workerMap, 4000000);
    EXPECT_EQ(3, tasks.size());
    EXPECT_EQ(1, tasks.count(make_pair("topic1", 1)));
    EXPECT_EQ(1, tasks.count(make_pair("topic3", 3)));
    EXPECT_EQ(1, tasks.count(make_pair("topic4", 4)));
    tasks.clear();

    tasks = _sysController->getNotTimeoutCurrentPartition(workerMap, 8000000);
    EXPECT_EQ(4, tasks.size());
    EXPECT_EQ(1, tasks.count(make_pair("topic1", 1)));
    EXPECT_EQ(1, tasks.count(make_pair("topic2", 2)));
    EXPECT_EQ(1, tasks.count(make_pair("topic3", 3)));
    EXPECT_EQ(1, tasks.count(make_pair("topic4", 4)));
    tasks.clear();

    _adminConfig.deadBrokerTimeoutSec = 9; // 9s
    tasks = _sysController->getNotTimeoutCurrentPartition(workerMap, 8000000);
    EXPECT_EQ(3, tasks.size());
    EXPECT_EQ(1, tasks.count(make_pair("topic1", 1)));
    EXPECT_EQ(1, tasks.count(make_pair("topic2", 2)));
    EXPECT_EQ(1, tasks.count(make_pair("topic4", 4)));
    tasks.clear();

    tasks = _sysController->getNotTimeoutCurrentPartition(workerMap, -1);
    EXPECT_EQ(4, tasks.size());
    EXPECT_EQ(1, tasks.count(make_pair("topic1", 1)));
    EXPECT_EQ(1, tasks.count(make_pair("topic2", 2)));
    EXPECT_EQ(1, tasks.count(make_pair("topic3", 3)));
    EXPECT_EQ(1, tasks.count(make_pair("topic4", 4)));
}

TEST_F(SysControllerTest, testDoRemoveOldHealthCheckData) {
    string basePath = GET_TEST_DATA_PATH();
    SysController sysController(nullptr, nullptr);
    string path = "mock://" + basePath + "/testdir";
    string fileName = path + "/_status_/0/default##broker_0";
    string content = StringUtil::toString(TimeUtility::currentTime());
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/0/default##broker_1";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/1/default##broker_1";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/1/default##broker_0";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/2/default##broker_1";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/2/default##broker_0";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/3/default##broker_1";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/3/default##broker_0";
    FileSystem::writeFile(fileName, content);
    fileName = path + "/_status_/badFileName";
    FileSystem::writeFile(fileName, content);

    FileSystem::_useMock = true;
    ExceptionTrigger::InitTrigger(0, true, false);
    ExceptionTrigger::SetExceptionCondition("listDir", path + "/_status_", fslib::EC_NOTSUP);
    EXPECT_FALSE(sysController.doRemoveOldHealthCheckData(path, "2", "3"));
    ExceptionTrigger::SetExceptionCondition("listDir", path + "/_status_", fslib::EC_OK);

    ExceptionTrigger::SetExceptionCondition("remove", path + "/_status_", fslib::EC_NOTEMPTY);
    EXPECT_FALSE(sysController.doRemoveOldHealthCheckData(path, "2", "3"));

    FileSystem::_useMock = false;
    vector<string> versions;
    FileSystem::listDir(path + "/_status_", versions);
    EXPECT_EQ(5, versions.size());
    EXPECT_TRUE(sysController.doRemoveOldHealthCheckData(path, "2", "3"));
    versions.clear();
    FileSystem::listDir(path + "/_status_", versions);
    EXPECT_EQ(2, versions.size());
    sort(versions.begin(), versions.end());
    EXPECT_EQ("2", versions[0]);
    EXPECT_EQ("3", versions[1]);
}

TEST_F(SysControllerTest, testSyncMasterVersion) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    {
        _adminConfig.backup = false;
        EXPECT_TRUE(_sysController->syncMasterVersion());
    }
    {
        _adminConfig.backup = true;
        EXPECT_EQ(0, _sysController->_selfMasterVersion);
        _sysController->_zkDataAccessor->writeMasterVersion(1234);
        EXPECT_FALSE(_sysController->syncMasterVersion());
        EXPECT_EQ(1234, _sysController->_selfMasterVersion);
    }
    {
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        std::vector<std::string> adminInfoPathVec;
        adminInfoPathVec.push_back(GET_TEMPLATE_DATA_PATH() + "/syncLeaderInfo/");
        _sysController->_adminConfig->syncAdminInfoPathVec = adminInfoPathVec;
        _adminConfig.backup = true;
        EXPECT_EQ(1234, _sysController->_selfMasterVersion);
        _sysController->_zkDataAccessor->writeMasterVersion(5678);
        EXPECT_TRUE(_sysController->syncMasterVersion());
        EXPECT_EQ(5678, _sysController->_selfMasterVersion);
    }
}

TEST_F(SysControllerTest, testSetMaster) {
    _sysController->setMaster(true);
    ASSERT_TRUE(_sysController->isMaster());
    _sysController->setMaster(false);
    ASSERT_FALSE(_sysController->isMaster());
}

TEST_F(SysControllerTest, testTopicInfoChanged) {
    SysController sysCtr(nullptr, nullptr);
    protocol::TopicInfo tinfo;
    TopicCreationRequest meta;
    EXPECT_TRUE(sysCtr.topicInfoChanged(tinfo, meta)); // default filed value diff

    tinfo.set_name("test");
    tinfo.set_partitioncount(1);
    tinfo.set_topicmode(TOPIC_MODE_SECURITY);
    tinfo.set_needfieldfilter(true);
    tinfo.set_obsoletefiletimeinterval(2);
    tinfo.set_reservedfilecount(3);
    tinfo.set_partitionminbuffersize(4);
    tinfo.set_partitionmaxbuffersize(5);
    tinfo.set_deletetopicdata(false);
    tinfo.set_maxwaittimeforsecuritycommit(6);
    tinfo.set_maxdatasizeforsecuritycommit(7);
    tinfo.set_compressmsg(false);
    tinfo.set_dfsroot("dfs1");
    tinfo.set_topicgroup("group");
    tinfo.set_sealed(false);
    tinfo.set_topictype(TOPIC_TYPE_NORMAL);
    tinfo.set_enablettldel(false);
    tinfo.set_readsizelimitsec(8);
    tinfo.set_enablelongpolling(false);
    tinfo.set_versioncontrol(false);
    tinfo.set_enablemergedata(false);
    tinfo.set_topicexpiredtime(9);
    tinfo.set_needschema(false);

    meta.set_partitioncount(1);
    meta.set_topicmode(TOPIC_MODE_SECURITY);
    meta.set_needfieldfilter(true);
    meta.set_obsoletefiletimeinterval(2);
    meta.set_reservedfilecount(3);
    meta.set_partitionminbuffersize(4);
    meta.set_partitionmaxbuffersize(5);
    meta.set_deletetopicdata(false);
    meta.set_maxwaittimeforsecuritycommit(6);
    meta.set_maxdatasizeforsecuritycommit(7);
    meta.set_compressmsg(false);
    meta.set_dfsroot("dfs1");
    meta.set_topicgroup("group");
    meta.set_sealed(false);
    meta.set_topictype(TOPIC_TYPE_NORMAL);
    meta.set_enablettldel(false);
    meta.set_readsizelimitsec(8);
    meta.set_enablelongpolling(false);
    meta.set_versioncontrol(false);
    meta.set_enablemergedata(false);
    meta.set_topicexpiredtime(9);
    meta.set_needschema(false);
    meta.set_topicname("test");

    EXPECT_FALSE(sysCtr.topicInfoChanged(tinfo, meta));
    meta.set_maxdatasizeforsecuritycommit(100);
    EXPECT_TRUE(sysCtr.topicInfoChanged(tinfo, meta));
    meta.set_maxdatasizeforsecuritycommit(7);
    *meta.add_extenddfsroot() = "dfs2";
    EXPECT_TRUE(sysCtr.topicInfoChanged(tinfo, meta));
    *tinfo.add_extenddfsroot() = "dfs2";
    EXPECT_FALSE(sysCtr.topicInfoChanged(tinfo, meta));
}

TEST_F(SysControllerTest, testFillTopicMeta) {
    SysController sysCtr(nullptr, nullptr);
    protocol::TopicInfo tinfo;
    TopicCreationRequest meta;

    tinfo.set_name("test");
    tinfo.set_partitioncount(1);
    tinfo.set_topicmode(TOPIC_MODE_SECURITY);
    tinfo.set_needfieldfilter(true);
    tinfo.set_obsoletefiletimeinterval(2);
    tinfo.set_reservedfilecount(3);
    tinfo.set_partitionminbuffersize(4);
    tinfo.set_partitionmaxbuffersize(5);
    tinfo.set_deletetopicdata(false);
    tinfo.set_maxwaittimeforsecuritycommit(6);
    tinfo.set_maxdatasizeforsecuritycommit(7);
    tinfo.set_compressmsg(false);
    tinfo.set_dfsroot("dfs1");
    tinfo.set_topicgroup("group");
    tinfo.set_sealed(false);
    tinfo.set_topictype(TOPIC_TYPE_NORMAL);
    tinfo.set_enablettldel(false);
    tinfo.set_readsizelimitsec(8);
    tinfo.set_enablelongpolling(false);
    tinfo.set_versioncontrol(false);
    tinfo.set_enablemergedata(false);
    tinfo.set_topicexpiredtime(9);
    tinfo.set_needschema(false);
    tinfo.add_extenddfsroot("dfs2");
    tinfo.add_extenddfsroot("dfs3");
    tinfo.add_schemaversions(123);
    tinfo.add_schemaversions(456);
    tinfo.add_physictopiclst("ptopic1");
    tinfo.add_physictopiclst("ptopic2");

    sysCtr.fillTopicMeta(tinfo, meta);
    EXPECT_FALSE(sysCtr.topicInfoChanged(tinfo, meta));
    EXPECT_EQ("test", meta.topicname());
    EXPECT_EQ(1, meta.partitioncount());
    EXPECT_EQ(TOPIC_MODE_SECURITY, meta.topicmode());
    EXPECT_EQ(true, meta.needfieldfilter());
    EXPECT_EQ(2, meta.obsoletefiletimeinterval());
    EXPECT_EQ(3, meta.reservedfilecount());
    EXPECT_EQ(4, meta.partitionminbuffersize());
    EXPECT_EQ(5, meta.partitionmaxbuffersize());
    EXPECT_EQ(false, meta.deletetopicdata());
    EXPECT_EQ(6, meta.maxwaittimeforsecuritycommit());
    EXPECT_EQ(7, meta.maxdatasizeforsecuritycommit());
    EXPECT_EQ(false, meta.compressmsg());
    EXPECT_EQ("dfs1", meta.dfsroot());
    EXPECT_EQ("group", meta.topicgroup());
    EXPECT_EQ(false, meta.sealed());
    EXPECT_EQ(TOPIC_TYPE_NORMAL, meta.topictype());
    EXPECT_EQ(false, meta.enablettldel());
    EXPECT_EQ(8, meta.readsizelimitsec());
    EXPECT_EQ(false, meta.enablelongpolling());
    EXPECT_EQ(false, meta.versioncontrol());
    EXPECT_EQ(false, meta.enablemergedata());
    EXPECT_EQ(9, meta.topicexpiredtime());
    EXPECT_EQ(false, meta.needschema());
    EXPECT_EQ("dfs2", meta.extenddfsroot(0));
    EXPECT_EQ("dfs3", meta.extenddfsroot(1));
    EXPECT_EQ(123, meta.schemaversions(0));
    EXPECT_EQ(456, meta.schemaversions(1));
    EXPECT_EQ("ptopic1", meta.physictopiclst(0));
    EXPECT_EQ("ptopic2", meta.physictopiclst(1));
}

TEST_F(SysControllerTest, testDoDiffTopics) {
    SysController sysCtr(nullptr, nullptr);
    TopicMap topicMap;
    AllTopicInfoResponse allResponse;

    TopicBatchCreationRequest newTopics;
    TopicBatchDeletionRequest deletedTopics;
    EXPECT_FALSE(sysCtr.doDiffTopics(topicMap, allResponse, newTopics, deletedTopics));
    EXPECT_EQ(0, newTopics.topicrequests_size());
    EXPECT_EQ(0, deletedTopics.topicnames_size());

    protocol::TopicInfo tinfo;
    TopicCreationRequest meta;
    TopicInfoPtr ti;

    // set common default attrs
    meta.set_obsoletefiletimeinterval(1);
    meta.set_reservedfilecount(1);
    meta.set_partitionminbuffersize(1);
    meta.set_partitionmaxbuffersize(1);
    meta.set_maxwaittimeforsecuritycommit(1);
    meta.set_maxdatasizeforsecuritycommit(1);
    meta.set_createtime(1);
    meta.set_topicgroup("group");
    meta.set_topicexpiredtime(1);
    meta.set_rangecountinpartition(1);
    meta.set_modifytime(1);
    tinfo.set_obsoletefiletimeinterval(1);
    tinfo.set_reservedfilecount(1);
    tinfo.set_partitionminbuffersize(1);
    tinfo.set_partitionmaxbuffersize(1);
    tinfo.set_maxwaittimeforsecuritycommit(1);
    tinfo.set_maxdatasizeforsecuritycommit(1);
    tinfo.set_createtime(1);
    tinfo.set_topicgroup("group");
    tinfo.set_topicexpiredtime(1);
    tinfo.set_rangecountinpartition(1);
    tinfo.set_modifytime(1);

    // 1. topic same
    meta.set_topicname("mainse");
    meta.set_partitioncount(2);
    meta.set_topicgroup("mainse");
    ti.reset(new TopicInfo(meta));
    topicMap["mainse"] = ti;
    tinfo.set_name("mainse");
    tinfo.set_partitioncount(2);
    tinfo.set_topicgroup("mainse");
    *allResponse.add_alltopicinfo() = tinfo;

    // 2. topic diff
    meta.set_topicname("tmall");
    meta.set_partitioncount(3);
    meta.set_topicmode(TOPIC_MODE_NORMAL);
    meta.add_owners("linson");
    meta.add_extenddfsroot("dfs1");
    ti.reset(new TopicInfo(meta));
    topicMap["tmall"] = ti;
    tinfo.set_name("tmall");
    tinfo.set_partitioncount(3);
    tinfo.set_topicmode(TOPIC_MODE_SECURITY); // diff
    tinfo.add_owners("linson");
    tinfo.add_extenddfsroot("dfs1");
    *allResponse.add_alltopicinfo() = tinfo;

    // 3. topic only in topic map
    meta.set_topicname("inshop");
    meta.set_partitioncount(4);
    ti.reset(new TopicInfo(meta));
    topicMap["inshop"] = ti;

    // 4. topic only in all response
    tinfo.set_name("order");
    tinfo.set_partitioncount(5);
    *allResponse.add_alltopicinfo() = tinfo;

    EXPECT_TRUE(sysCtr.doDiffTopics(topicMap, allResponse, newTopics, deletedTopics));
    EXPECT_EQ(2, newTopics.topicrequests_size());
    auto nmeta = newTopics.topicrequests(0);
    EXPECT_EQ("tmall", nmeta.topicname());
    EXPECT_EQ(TOPIC_MODE_SECURITY, nmeta.topicmode());
    nmeta = newTopics.topicrequests(1);
    EXPECT_EQ("order", nmeta.topicname());
    EXPECT_EQ(5, nmeta.partitioncount());

    EXPECT_EQ(2, deletedTopics.topicnames_size());
    EXPECT_EQ("tmall", deletedTopics.topicnames(0));
    EXPECT_EQ("inshop", deletedTopics.topicnames(1));
}

TEST_F(SysControllerTest, testGetTopicInfo) {
    TopicCreationRequest topicCreateRequest;
    TopicCreationResponse topicCreateResponse;
    _sysController->setPrimary(true);
    _sysController->_moduleManager->loadModule<TopicAclManageModule>();
    auto topicAclModule = _sysController->_moduleManager->getModule<TopicAclManageModule>();
    topicAclModule->update(_sysController->_adminStatusManager._currentStatus);
    string topicName = "topic_4";
    topicCreateRequest.set_topicname(topicName);
    topicCreateRequest.set_partitioncount(4);
    topicCreateRequest.set_partitionmaxbuffersize(400);
    _sysController->createTopic(&topicCreateRequest, &topicCreateResponse);
    ASSERT_EQ(protocol::ERROR_NONE, topicCreateResponse.errorinfo().errcode());

    TopicInfoRequest topicInfoRequest;
    topicInfoRequest.set_topicname(topicName);
    TopicInfoResponse topicInfoResponse;
    _sysController->setMaster(false);
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    ASSERT_FALSE(_sysController->isMaster());
    EXPECT_EQ(ERROR_ADMIN_NOT_LEADER, topicInfoResponse.errorinfo().errcode());

    topicInfoRequest.set_src("tpp");
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    EXPECT_EQ(ERROR_ADMIN_NOT_LEADER, topicInfoResponse.errorinfo().errcode());

    topicInfoRequest.set_src("api");
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    EXPECT_EQ(ERROR_NONE, topicInfoResponse.errorinfo().errcode());
    protocol::TopicInfo ti = topicInfoResponse.topicinfo();
    ASSERT_EQ(TOPIC_MODE_NORMAL, ti.topicmode());
    ASSERT_EQ(4, ti.partitioncount());
    ASSERT_EQ(400, ti.partitionmaxbuffersize());

    topicInfoRequest.set_src("tpp");
    _sysController->setMaster(true);
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    EXPECT_EQ(ERROR_NONE, topicInfoResponse.errorinfo().errcode());

    topicInfoRequest.set_topicname("not_exist");
    _sysController->getTopicInfo(&topicInfoRequest, &topicInfoResponse);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, topicInfoResponse.errorinfo().errcode());

    topicAclModule.reset();
    _sysController->_moduleManager->unLoadModule<TopicAclManageModule>();
}

// TEST_F(SysControllerTest, testUpdateWriterVersion) {
//     UpdateWriterVersionRequest request;
//     UpdateWriterVersionResponse response;

//     // 1. not leader
//     _sysController->setPrimary(false);
//     _sysController->updateWriterVersion(&request, &response);
//     ASSERT_EQ(ERROR_ADMIN_NOT_LEADER, response.errorinfo().errcode());

//     // 2. topicWriterController empty
//     _sysController->setPrimary(true);
//     _sysController->updateWriterVersion(&request, &response);
//     ASSERT_EQ(ERROR_ADMIN_OPERATION_FAILED, response.errorinfo().errcode());

//     // 3. request vaildate fail
//     _sysController->_topicWriterController.reset(new TopicWriterController(_sysController->_zkDataAccessor,
//     _zkRoot)); _sysController->updateWriterVersion(&request, &response); ASSERT_EQ(ERROR_CLIENT_INVALID_PARAMETERS,
//     response.errorinfo().errcode());

//     // 4. request topic not in _topicTable
//     request.mutable_topicwriterversion()->set_topicname("test_topic");
//     request.mutable_topicwriterversion()->set_majorversion(100);
//     WriterVersion version;
//     version.set_name("processor_2_200");
//     version.set_version(200);
//     *request.mutable_topicwriterversion()->add_writerversions() = version;
//     _sysController->updateWriterVersion(&request, &response);
//     ASSERT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());

//     // 5. not enable version controll
//     TopicCreationRequest meta;
//     meta.set_topicname("test_topic");
//     meta.set_partitioncount(1);
//     _sysController->_topicTable.addTopic(&meta);
//     _sysController->updateWriterVersion(&request, &response);
//     ASSERT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, response.errorinfo().errcode());

//     // 6. success
//     meta.set_versioncontrol(true);
//     _sysController->_topicTable.updateTopic(&meta);
//     _sysController->updateWriterVersion(&request, &response);
//     ASSERT_EQ(ERROR_NONE, response.errorinfo().errcode());
// }

TEST_F(SysControllerTest, testLoadOldTopicMeta) {
    string topicName = "aaaaa";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    request.set_deletetopicdata(true);
    ASSERT_TRUE(_sysController->_zkDataAccessor->addTopic(request));
    ASSERT_TRUE(_sysController->loadTopicInfo());
    TopicInfoPtr topicInfoPtr = _sysController->_topicTable.findTopic(topicName);
    ASSERT_EQ(topicName, topicInfoPtr->getTopicMeta().topicname());
    ASSERT_FALSE(topicInfoPtr->getTopicMeta().deletetopicdata());
}

TEST_F(SysControllerTest, testFillTopicOpControl) {
    {
        TopicAccessInfo accessInfo;
        TopicInfoResponse response;
        accessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
        SysController sysCtr(nullptr, nullptr);
        sysCtr.fillTopicOpControl(&response, accessInfo);
        EXPECT_TRUE(response.topicinfo().opcontrol().canread());
        EXPECT_TRUE(response.topicinfo().opcontrol().canwrite());
    }
    {
        TopicAccessInfo accessInfo;
        TopicInfoResponse response;
        accessInfo.set_accesstype(TOPIC_ACCESS_READ);
        SysController sysCtr(nullptr, nullptr);
        sysCtr.fillTopicOpControl(&response, accessInfo);
        EXPECT_TRUE(response.topicinfo().opcontrol().canread());
        EXPECT_FALSE(response.topicinfo().opcontrol().canwrite());
    }
    {
        TopicAccessInfo accessInfo;
        TopicInfoResponse response;
        accessInfo.set_accesstype(TOPIC_ACCESS_NONE);
        SysController sysCtr(nullptr, nullptr);
        sysCtr.fillTopicOpControl(&response, accessInfo);
        EXPECT_FALSE(response.topicinfo().opcontrol().canread());
        EXPECT_FALSE(response.topicinfo().opcontrol().canwrite());
    }
}

} // namespace admin
} // namespace swift
