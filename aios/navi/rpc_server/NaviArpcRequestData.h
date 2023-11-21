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

#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "navi/log/NaviLogger.h"
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace navi {

class NaviArpcRequestData : public Data, public std::enable_shared_from_this<NaviArpcRequestData> {
public:
    NaviArpcRequestData(const google::protobuf::Message *request,
                        google::protobuf::Message *needDeleteRequest,
                        const std::string &methodName,
                        const std::string &clientIp);
    ~NaviArpcRequestData();
    NaviArpcRequestData(const NaviArpcRequestData &) = delete;
    NaviArpcRequestData &operator=(const NaviArpcRequestData &) = delete;
public:
    static const std::string TYPE_ID;
public:
    const google::protobuf::Message *getRequest() const;
    template<typename T>
    const T *getRequest() const;
    const std::string &getMethodName() const {
        return _methodName;
    }
    const std::string &getClientIp() const {
        return _clientIp;
    }
    void setAiosDebug(bool aiosDebug) {
        _aiosDebug = aiosDebug;
    }
    bool isAiosDebug() const {
        return _aiosDebug;
    }
private:
    const google::protobuf::Message *_request = nullptr;
    google::protobuf::Message *_needDeleteRequest = nullptr;
    std::string _methodName;
    std::string _clientIp;
    bool _aiosDebug = false;
};

template<typename T>
const T *NaviArpcRequestData::getRequest() const {
    auto request = getRequest();
    auto msg =dynamic_cast<const T *>(request);
    if (!msg) {
        if (request) {
            NAVI_KERNEL_LOG(ERROR, "invalid pb request type [%s]",
                            request->GetDescriptor()->full_name().c_str());
        } else {
            NAVI_KERNEL_LOG(ERROR, "null pb request");
        }
    }
    return msg;
}

class NaviArpcRequestType : public Type {
public:
    NaviArpcRequestType();
    ~NaviArpcRequestType();
public:
    navi::TypeErrorCode serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const override;
    navi::TypeErrorCode deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const override;
public:
    static google::protobuf::Message *
    parseProtobufMesasge(const std::string &typeName,
                         const autil::StringView &valueStr);
};

}
