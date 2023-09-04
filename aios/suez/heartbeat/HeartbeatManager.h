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

#include <string>

#include "autil/Lock.h"
#include "autil/legacy/json.h"
#include "suez/common/TargetRecorder.h"
#include "suez/heartbeat/SuezHB.pb.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

namespace suez {

class HeartbeatTarget;
class RpcServer;
class SchedulerInfo;

class HeartbeatManager : public HeartbeatService {
public:
    HeartbeatManager();
    ~HeartbeatManager();

private:
    HeartbeatManager(const HeartbeatManager &);
    HeartbeatManager &operator=(const HeartbeatManager &);

public:
    void heartbeat(::google::protobuf::RpcController *controller,
                   const TargetRequest *request,
                   CurrentResponse *response,
                   ::google::protobuf::Closure *done) override;
    void setLoggerLevel(::google::protobuf::RpcController *controller,
                        const ::suez::SetLoggerLevelRequest *request,
                        ::suez::SetLoggerLevelResponse *response,
                        ::google::protobuf::Closure *done) override;
    void current(::google::protobuf::RpcController *controller,
                 const CurrentRequest *request,
                 CurrentResponse *response,
                 ::google::protobuf::Closure *done) override;

public:
    bool init(RpcServer *server,
              const std::string &carbonIdentifier,
              const std::string &procInstanceId,
              const std::string &packageCheckSum);
    void setCurrent(const autil::legacy::json::JsonMap &current,
                    const autil::legacy::json::JsonMap &serviceInfo,
                    const std::string &signature);
    bool getTarget(HeartbeatTarget &target, HeartbeatTarget &finalTarget, SchedulerInfo &schedulerInfo) const;

private:
    bool getGlobalCustomInfo(const std::string &globalCustomInfo);
    bool processVersionMatch(const std::string &processVersion);

private:
    mutable autil::ThreadMutex _mutex;
    std::string _currentSnapShot;
    std::string _targetSnapShot;
    std::string _finalTargetSnapShot;
    std::string _signature;
    std::string _currentSignature;
    std::string _currentServiceInfo;
    TargetRecorder _targetRecorder;
    std::string _carbonIdentifier;
    std::string _procInstanceId;
    std::string _packageCheckSum;
    std::string _schedulerInfoStr;
    bool _preload;

private:
    static const std::string CUSTOM_INFO;
};

} // namespace suez
