#include "swift/util/ProtoUtil.h"

#include <iosfwd>
#include <string>

#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;

namespace swift {
namespace util {

class ProtoUtilTest : public TESTBASE {};

TEST_F(ProtoUtilTest, testPlainDiffStr) {
    TopicInfo t1, t2;
    // empty diff
    EXPECT_EQ("{}", ProtoUtil::plainDiffStr(&t1, &t2));

    // multi diff
    t1.set_name("topic1");
    t1.set_partitioncount(2);
    t1.add_owners("u1");
    t1.add_owners("u3");
    t1.set_sealed(false);
    t1.set_topictype(TOPIC_TYPE_NORMAL);
    t2.set_name("topic2");
    t2.set_partitioncount(4);
    t2.add_owners("u1");
    t2.add_owners("u2");
    t2.add_extenddfsroot("dfs1");
    t2.add_extenddfsroot("dfs2");
    t2.set_sealed(true);
    t2.set_topictype(TOPIC_TYPE_LOGIC_PHYSIC);
    const string &ret = ProtoUtil::plainDiffStr(&t1, &t2);
    EXPECT_EQ("{name[topic1->topic2] partitionCount[2->4] "
              "extendDfsRoot[->dfs1,dfs2,] owners[u1,u3,->u1,u2,] "
              "sealed[false->true] "
              "topicType[TOPIC_TYPE_NORMAL->TOPIC_TYPE_LOGIC_PHYSIC] }",
              ret);

    // owners diff
    t2.set_name("topic1");
    t2.set_partitioncount(2);
    t2.add_owners("u3");
    t1.add_owners("u2");
    t2.clear_extenddfsroot();
    t2.set_sealed(false);
    t2.set_topictype(TOPIC_TYPE_NORMAL);
    const string &ret2 = ProtoUtil::plainDiffStr(&t1, &t2);
    EXPECT_EQ("{owners[u1,u3,u2,->u1,u2,u3,] }", ret2);

    // all same
    t1.clear_owners();
    t2.clear_owners();
    t1.add_owners("u4");
    t2.add_owners("u4");
    const string &ret3 = ProtoUtil::plainDiffStr(&t1, &t2);
    EXPECT_EQ("{}", ret3);
}

TEST_F(ProtoUtilTest, testGetHeartbeatStr) {
    HeartbeatInfo heartbeat;
    const string &hbStr = ProtoUtil::getHeartbeatStr(heartbeat);
    EXPECT_EQ("taskSize:0 address: alive:false sessionId:0 tasks:{}", hbStr);
    heartbeat.set_alive(false);
    heartbeat.set_address("broker_1##10.1.1:100");
    heartbeat.set_sessionid(1617854155394204);
    TaskStatus *task = heartbeat.add_tasks();
    TaskInfo *tinfo = task->mutable_taskinfo();
    PartitionId *partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic");
    partId->set_id(100);
    partId->set_topicgroup("default");
    task->set_status(PARTITION_STATUS_STOPPING);

    task = heartbeat.add_tasks();
    tinfo = task->mutable_taskinfo();
    partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic2");
    partId->set_id(20);
    partId->set_partitioncount(50);
    partId->set_from(111);
    partId->set_to(333);
    partId->set_topicgroup("igraph");
    partId->set_version(123456789);
    auto *inlineVersion = partId->mutable_inlineversion();
    inlineVersion->set_masterversion(0);
    inlineVersion->set_partversion(111);
    task->set_status(PARTITION_STATUS_RUNNING);
    task->set_sessionid(100);

    const string &hbStr2 = ProtoUtil::getHeartbeatStr(heartbeat);
    EXPECT_EQ("taskSize:2 address:broker_1##10.1.1:100 alive:false "
              "sessionId:1617854155394204 tasks:{[topic 100/0(0-0) "
              "default 0 0-0 PARTITION_STATUS_STOPPING -1] [topic2 20/50(111-333) "
              "igraph 123456789 0-111 PARTITION_STATUS_RUNNING 100] }",
              hbStr2);
}

TEST_F(ProtoUtilTest, testGetDispatchStr) {
    DispatchInfo dispatchInfo;
    const string &diEmptyStr = ProtoUtil::getDispatchStr(dispatchInfo);
    EXPECT_EQ("[:] sessionId:0 taskSize:0 tasks:{}", diEmptyStr);

    dispatchInfo.set_rolename("default##broker_1_0");
    dispatchInfo.set_brokeraddress("10.1.2.3");
    TaskInfo *tinfo = dispatchInfo.add_taskinfos();
    PartitionId *partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic1");
    partId->set_id(100);
    partId->set_topicgroup("default");
    tinfo->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    tinfo->set_obsoletefiletimeinterval(123);
    tinfo->set_maxbuffersize(456);
    tinfo->set_compressmsg(true);
    tinfo->set_sealed(false);
    tinfo->set_enablettldel(false);

    tinfo = dispatchInfo.add_taskinfos();
    partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic2");
    partId->set_id(200);
    partId->set_topicgroup("igraph");
    auto *inlineVersion = partId->mutable_inlineversion();
    inlineVersion->set_masterversion(0);
    inlineVersion->set_partversion(111);
    tinfo->set_topicmode(TOPIC_MODE_NORMAL);
    tinfo->set_obsoletefiletimeinterval(321);
    tinfo->set_maxbuffersize(654);
    tinfo->set_compressmsg(false);
    tinfo->set_sealed(false);
    tinfo->set_enablettldel(true);
    tinfo->set_readsizelimitsec(200);
    tinfo->set_enablelongpolling(true);
    tinfo->set_readnotcommittedmsg(false);

    const string &diStr = ProtoUtil::getDispatchStr(dispatchInfo);
    EXPECT_EQ("[default##broker_1_0:10.1.2.3] "
              "sessionId:0 taskSize:2 tasks:{[topic1 100/0(0-0) default 0 0-0 "
              "TOPIC_MODE_MEMORY_ONLY false 123 456 true false false -1 false false true] "
              "[topic2 200/0(0-0) igraph 0 0-111 TOPIC_MODE_NORMAL false 321 654 "
              "false false true 200 true false false] }",
              diStr);
}

TEST_F(ProtoUtilTest, testGetTaskInfoStr) {
    TaskInfo taskInfo;
    TaskInfo *tinfo = &taskInfo;
    PartitionId *partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic1");
    partId->set_id(100);
    partId->set_from(10);
    partId->set_to(50);
    partId->set_partitioncount(102);
    partId->set_topicgroup("default");
    tinfo->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
    tinfo->set_obsoletefiletimeinterval(123);
    tinfo->set_maxbuffersize(456);
    tinfo->set_compressmsg(true);
    tinfo->set_sealed(false);
    tinfo->set_enablettldel(false);
    const string &diStr = ProtoUtil::getTaskInfoStr(taskInfo);
    EXPECT_EQ("[topic1 100/102(10-50) default 0 0-0 TOPIC_MODE_MEMORY_ONLY "
              "false 123 456 true false false -1 false false true] ",
              diStr);

    TaskInfo taskInfo2;
    tinfo = &taskInfo2;
    partId = tinfo->mutable_partitionid();
    partId->set_topicname("topic2");
    partId->set_id(10);
    partId->set_partitioncount(102);
    partId->set_topicgroup("igraph");
    tinfo->set_topicmode(TOPIC_MODE_NORMAL);
    tinfo->set_obsoletefiletimeinterval(321);
    tinfo->set_maxbuffersize(654);
    tinfo->set_compressmsg(false);
    tinfo->set_sealed(false);
    tinfo->set_enablettldel(true);
    tinfo->set_readsizelimitsec(200);
    tinfo->set_enablelongpolling(true);
    tinfo->set_versioncontrol(true);
    tinfo->set_readnotcommittedmsg(false);

    const string &diStr2 = ProtoUtil::getTaskInfoStr(taskInfo2);
    EXPECT_EQ("[topic2 10/102(0-0) igraph 0 0-0 TOPIC_MODE_NORMAL "
              "false 321 654 false false true 200 true true false] ",
              diStr2);
}

TEST_F(ProtoUtilTest, testGetAllTopicInfoStr) {
    AllTopicInfoResponse allTopicInfo;
    string resStr = ProtoUtil::getAllTopicInfoStr(allTopicInfo);
    EXPECT_EQ("ERROR_NONE", resStr);

    protocol::TopicInfo tinfo;
    tinfo.set_name("tmall");
    tinfo.set_partitioncount(1);
    *allTopicInfo.add_alltopicinfo() = tinfo;

    tinfo.set_obsoletefiletimeinterval(1);
    tinfo.set_reservedfilecount(2);
    tinfo.set_partitionminbuffersize(2);
    tinfo.set_partitionmaxbuffersize(4);
    tinfo.set_maxwaittimeforsecuritycommit(5);
    tinfo.set_maxdatasizeforsecuritycommit(6);
    tinfo.set_createtime(100);
    tinfo.set_topicgroup("group");
    tinfo.set_topicexpiredtime(200);
    tinfo.set_modifytime(300);
    tinfo.set_name("mainse");
    tinfo.set_partitioncount(2);
    tinfo.set_topicgroup("mainse");
    tinfo.set_readnotcommittedmsg(false);
    *allTopicInfo.add_alltopicinfo() = tinfo;
    allTopicInfo.mutable_errorinfo()->set_errcode(ERROR_WORKER_DEAD);

    resStr = ProtoUtil::getAllTopicInfoStr(allTopicInfo);
    EXPECT_EQ("ERROR_WORKER_DEAD [name:tmall status:0 partitionCount:1 topicMode:0 needFieldFilter:false "
              "obsoleteFileTimeInterval:0 reservedFileCount:0 minBufferSize:0 maxBufferSize:0 maxWaitTimeForSC:0 "
              "maxDataSizeForSC:0 createTime:0 modifyTime:0 topicExpiredTime:0 sealed:false topicType:0 "
              "needSchema:false enableTTLDel:true readSizeLimitSec:-1 versionControl:false enableLongPolling:false "
              "enableMergeData:false readNotCommittedMsg:true dfsRoot: permission:truetruefalsefalse extendDfsRoot:() "
              "owners:() schemaVersions:() "
              "physicTopicLst:()] [name:mainse status:0 partitionCount:2 topicMode:0 needFieldFilter:false "
              "obsoleteFileTimeInterval:1 reservedFileCount:2 minBufferSize:2 maxBufferSize:4 maxWaitTimeForSC:5 "
              "maxDataSizeForSC:6 createTime:100 modifyTime:300 topicExpiredTime:200 sealed:false topicType:0 "
              "needSchema:false enableTTLDel:true readSizeLimitSec:-1 versionControl:false enableLongPolling:false "
              "enableMergeData:false readNotCommittedMsg:false dfsRoot: permission:truetruefalsefalse extendDfsRoot:() "
              "owners:() schemaVersions:() physicTopicLst:()]",
              resStr);
}

} // namespace util
} // namespace swift
