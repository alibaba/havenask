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
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "carbon/RoleManagerWrapper.h"
#include "suez/admin/Admin.pb.h"
#include "suez/common/TargetRecorder.h"
#include "suez/sdk/JsonNodeRef.h"

namespace worker_framework {
class WorkerState;
} // namespace worker_framework

namespace suez {
using WorkerState = worker_framework::WorkerState;

class AdminServiceImpl : public AdminService {
public:
    AdminServiceImpl();
    ~AdminServiceImpl();

private:
    AdminServiceImpl(const AdminServiceImpl &);
    AdminServiceImpl &operator=(const AdminServiceImpl &);

public:
    bool start(bool isNewStart,
               const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager,
               std::unique_ptr<WorkerState> state);
    void stop();

public:
    void create(::google::protobuf::RpcController *controller,
                const ::suez::CreateRequest *request,
                ::suez::Response *response,
                ::google::protobuf::Closure *done) override;
    void update(::google::protobuf::RpcController *controller,
                const ::suez::UpdateRequest *request,
                ::suez::Response *response,
                ::google::protobuf::Closure *done) override;
    void read(::google::protobuf::RpcController *controller,
              const ::suez::ReadRequest *request,
              ::suez::ReadResponse *response,
              ::google::protobuf::Closure *done) override;
    void del(::google::protobuf::RpcController *controller,
             const ::suez::DeleteRequest *request,
             ::suez::Response *response,
             ::google::protobuf::Closure *done) override;
    void batch(::google::protobuf::RpcController *controller,
               const ::suez::BatchRequest *request,
               ::suez::Response *response,
               ::google::protobuf::Closure *done) override;
    void releaseSlot(::google::protobuf::RpcController *controller,
                     const ::suez::ReleaseSlotRequest *request,
                     ::suez::Response *response,
                     ::google::protobuf::Closure *done) override;
    void setLoggerLevel(::google::protobuf::RpcController *controller,
                        const ::suez::SetLoggerLevelRequest *request,
                        ::suez::SetLoggerLevelResponse *response,
                        ::google::protobuf::Closure *done) override;

public:
    std::pair<JsonNodeRef::JsonMap, std::string> getTarget() const;

private:
    template <typename RequestType>
    void operate(const RequestType *request,
                 ::suez::Response *response,
                 const std::function<bool(const RequestType *, ::suez::Response *, JsonNodeRef::JsonMap &)> &func,
                 ::google::protobuf::Closure *done);
    bool doRead(const ReadRequest *request, ReadResponse *response);
    static bool doCreate(const CreateRequest *request, ::suez::Response *response, JsonNodeRef::JsonMap &target);
    static bool doUpdate(const UpdateRequest *request, ::suez::Response *response, JsonNodeRef::JsonMap &target);
    static bool doDel(const DeleteRequest *request, ::suez::Response *response, JsonNodeRef::JsonMap &target);
    static bool doBatch(const BatchRequest *request, ::suez::Response *response, JsonNodeRef::JsonMap &target);
    bool serialize(const std::string &targetStr);
    bool deserialize(JsonNodeRef::JsonMap &target, std::string &targetStr);

    void commitToCarbon();
    void genTarget();
    static void logAccess(const std::string &operate, bool success, const std::string &requestStr, int64_t startTime);
    static void reRangeGroupStatus(std::map<std::string, carbon::GroupStatus> &groupStatusMap);
    bool getSystemInfo(const std::string &path, JsonNodeRef::JsonMap &target, bool minimalGsMode);

    void setCommitError(const std::string &err);
    std::string getCommitError() const;

    void setTarget(const JsonNodeRef::JsonMap &target, std::string targetStr);
    const JsonNodeRef::JsonMap &getTargetNoLock() const;

    static bool compress(std::string &content);
    static bool decompress(std::string &content);

    static void simplifyGroupsStatus(std::map<std::string, carbon::GroupStatus> &groupStatusMap);

private:
    static const std::string SUEZ_STATUS;
    static const std::string SYSTEM_INFO;
    static const std::string ERROR;

private:
    std::atomic<bool> _serviceReady;

    JsonNodeRef::JsonMap _target;
    std::string _lastSerializedStr;
    mutable autil::ReadWriteLock _targetLock;

    TargetRecorder _targetRecorder;
    mutable autil::ThreadMutex _operateLock;

    autil::LoopThreadPtr _loopThread;
    std::string _lastCommitStr;
    std::string _lastCommitError;
    mutable autil::ThreadMutex _commitErrorLock;

    std::shared_ptr<carbon::RoleManagerWrapper> _roleManager;
    std::unique_ptr<WorkerState> _state;
    bool _minimalGsMode;
};

} // namespace suez
