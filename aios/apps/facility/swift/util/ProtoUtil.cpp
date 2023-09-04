/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "swift/util/ProtoUtil.h"

#include <google/protobuf/message.h>
#include <sstream>
#include <stddef.h>
#include <stdint.h>

#include "google/protobuf/descriptor.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

using namespace std;
using namespace ::google::protobuf;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, ProtoUtil);

ProtoUtil::ProtoUtil() {}

ProtoUtil::~ProtoUtil() {}

string ProtoUtil::plainDiffStr(const google::protobuf::Message *msg1, const google::protobuf::Message *msg2) {
#define GET_Enum(a) (a->name())
#define GET_Int32(a) (a)
#define GET_UInt32(a) (a)
#define GET_Int64(a) (a)
#define GET_UInt64(a) (a)
#define GET_Double(a) (a)
#define GET_Float(a) (a)
#define GET_String(a) (a)
#define GET_Bool(a) (a)

#define DIFF_FIELD(cppType, funcSuffix)                                                                                \
    case cppType: {                                                                                                    \
        if (fieldDesc1->is_repeated()) {                                                                               \
            size_t count1 = ref1->FieldSize(*msg1, fieldDesc1);                                                        \
            size_t count2 = ref2->FieldSize(*msg2, fieldDesc2);                                                        \
            bool same = true;                                                                                          \
            if (count1 != count2) {                                                                                    \
                same = false;                                                                                          \
            } else {                                                                                                   \
                for (size_t i = 0; i < count1; ++i) {                                                                  \
                    auto val1 = GET_##funcSuffix(ref1->GetRepeated##funcSuffix(*msg1, fieldDesc1, i));                 \
                    auto val2 = GET_##funcSuffix(ref2->GetRepeated##funcSuffix(*msg2, fieldDesc2, i));                 \
                    if (val1 != val2) {                                                                                \
                        same = false;                                                                                  \
                        break;                                                                                         \
                    }                                                                                                  \
                }                                                                                                      \
            }                                                                                                          \
            if (!same) {                                                                                               \
                ostringstream s1, s2;                                                                                  \
                for (size_t i = 0; i < count1; ++i) {                                                                  \
                    auto val = GET_##funcSuffix(ref1->GetRepeated##funcSuffix(*msg1, fieldDesc1, i));                  \
                    s1 << std::boolalpha << val << ",";                                                                \
                }                                                                                                      \
                for (size_t i = 0; i < count2; ++i) {                                                                  \
                    auto val = GET_##funcSuffix(ref2->GetRepeated##funcSuffix(*msg2, fieldDesc2, i));                  \
                    s2 << std::boolalpha << val << ",";                                                                \
                }                                                                                                      \
                ret << fieldName << "[" << s1.str() << "->" << s2.str() << "] ";                                       \
            }                                                                                                          \
        } else {                                                                                                       \
            auto val1 = GET_##funcSuffix(ref1->Get##funcSuffix(*msg1, fieldDesc1));                                    \
            auto val2 = GET_##funcSuffix(ref2->Get##funcSuffix(*msg2, fieldDesc2));                                    \
            if (val1 != val2) {                                                                                        \
                ret << fieldName << "[" << val1 << "->" << val2 << "] ";                                               \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

    ostringstream ret;
    ret << std::boolalpha << "{";
    const auto *desc1 = msg1->GetDescriptor();
    const auto *desc2 = msg2->GetDescriptor();
    const auto *ref1 = msg1->GetReflection();
    const auto *ref2 = msg2->GetReflection();
    size_t fieldCount = desc1->field_count();
    for (size_t i = 0; i < fieldCount; i++) {
        const auto *fieldDesc1 = desc1->field(i);
        const auto *fieldDesc2 = desc2->field(i);
        const string &fieldName = fieldDesc1->name();
        switch (fieldDesc1->cpp_type()) {
            DIFF_FIELD(FieldDescriptor::CPPTYPE_INT32, Int32);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_UINT32, UInt32);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_INT64, Int64);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_UINT64, UInt64);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_DOUBLE, Double);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_FLOAT, Float);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_STRING, String);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_BOOL, Bool);
            DIFF_FIELD(FieldDescriptor::CPPTYPE_ENUM, Enum);
            // plain only (not suppport FieldDescriptor::CPPTYPE_MESSAGE)
        default: {
            break;
        }
        }
    }
    ret << "}";
    return ret.str();
}

string ProtoUtil::getHeartbeatStr(const protocol::HeartbeatInfo &msg) {
    int taskSize = msg.tasks_size();
    ostringstream oss;
    oss << boolalpha << "taskSize:" << taskSize << " address:" << msg.address() << " alive:" << msg.alive()
        << " sessionId:" << msg.sessionid() << " tasks:{";
    for (int32_t i = 0; i < taskSize; i++) {
        const auto &partId = msg.tasks(i).taskinfo().partitionid();
        oss << "[" << partId.topicname() << " " << partId.id() << "/" << partId.partitioncount() << "(" << partId.from()
            << "-" << partId.to() << ") " << partId.topicgroup() << " " << partId.version() << " "
            << partId.inlineversion().masterversion() << "-" << partId.inlineversion().partversion() << " "
            << PartitionStatus_Name(msg.tasks(i).status()) << " " << msg.tasks(i).sessionid() << "] ";
    }
    oss << "}";
    return oss.str();
}

string ProtoUtil::getDispatchStr(const protocol::DispatchInfo &msg) {
    int taskSize = msg.taskinfos_size();
    ostringstream oss;
    oss << boolalpha << "[" << msg.rolename() << ":" << msg.brokeraddress() << "] sessionId:" << msg.sessionid()
        << " taskSize:" << taskSize << " tasks:{";
    for (int32_t i = 0; i < taskSize; i++) {
        oss << getTaskInfoStr(msg.taskinfos(i));
    }
    oss << "}";
    return oss.str();
}

string ProtoUtil::getTaskInfoStr(const protocol::TaskInfo &ti) {
    ostringstream oss;
    oss << boolalpha;
    const auto &partId = ti.partitionid();
    oss << "[" << partId.topicname() << " " << partId.id() << "/" << partId.partitioncount() << "(" << partId.from()
        << "-" << partId.to() << ")"
        << " " << partId.topicgroup() << " " << partId.version() << " " << partId.inlineversion().masterversion() << "-"
        << partId.inlineversion().partversion() << " " << TopicMode_Name(ti.topicmode()) << " " << ti.needfieldfilter()
        << " " << ti.obsoletefiletimeinterval() << " " << ti.maxbuffersize() << " " << ti.compressmsg() << " "
        << ti.sealed() << " " << ti.enablettldel() << " " << ti.readsizelimitsec() << " " << ti.enablelongpolling()
        << " " << ti.versioncontrol() << " " << ti.readnotcommittedmsg() << "] ";
    return oss.str();
}

string ProtoUtil::getAllTopicInfoStr(const protocol::AllTopicInfoResponse &allTopicInfo) {
    ostringstream oss;
    oss << boolalpha;
    oss << protocol::ErrorCode_Name(allTopicInfo.errorinfo().errcode());
    for (int idx = 0; idx < allTopicInfo.alltopicinfo_size(); ++idx) {
        const auto &tinfo = allTopicInfo.alltopicinfo(idx);
        oss << " [name:" << tinfo.name() << " status:" << tinfo.status() << " partitionCount:" << tinfo.partitioncount()
            << " topicMode:" << tinfo.topicmode() << " needFieldFilter:" << tinfo.needfieldfilter()
            << " obsoleteFileTimeInterval:" << tinfo.obsoletefiletimeinterval()
            << " reservedFileCount:" << tinfo.reservedfilecount() << " minBufferSize:" << tinfo.partitionminbuffersize()
            << " maxBufferSize:" << tinfo.partitionmaxbuffersize()
            << " maxWaitTimeForSC:" << tinfo.maxwaittimeforsecuritycommit()
            << " maxDataSizeForSC:" << tinfo.maxdatasizeforsecuritycommit() << " createTime:" << tinfo.createtime()
            << " modifyTime:" << tinfo.modifytime() << " topicExpiredTime:" << tinfo.topicexpiredtime()
            << " sealed:" << tinfo.sealed() << " topicType:" << tinfo.topictype()
            << " needSchema:" << tinfo.needschema() << " enableTTLDel:" << tinfo.enablettldel()
            << " readSizeLimitSec:" << tinfo.readsizelimitsec() << " versionControl:" << tinfo.versioncontrol()
            << " enableLongPolling:" << tinfo.enablelongpolling() << " enableMergeData:" << tinfo.enablemergedata()
            << " readNotCommittedMsg:" << tinfo.readnotcommittedmsg() << " dfsRoot:" << tinfo.dfsroot()
            << " permission:" << tinfo.opcontrol().canread() << tinfo.opcontrol().canwrite()
            << tinfo.opcontrol().canmodify() << tinfo.opcontrol().candelete();
        oss << " extendDfsRoot:(";
        for (int i = 0; i < tinfo.extenddfsroot_size(); ++i) {
            oss << tinfo.extenddfsroot(i) << " ";
        }
        oss << ") owners:(";
        for (int i = 0; i < tinfo.owners_size(); ++i) {
            oss << tinfo.owners(i) << " ";
        }
        oss << ") schemaVersions:(";
        for (int i = 0; i < tinfo.schemaversions_size(); ++i) {
            oss << tinfo.schemaversions(i) << " ";
        }
        oss << ") physicTopicLst:(";
        for (int i = 0; i < tinfo.physictopiclst_size(); ++i) {
            oss << tinfo.physictopiclst(i) << " ";
        }
        oss << ")]";
    }
    return oss.str();
}

} // namespace util
} // namespace swift
