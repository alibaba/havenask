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
#include "navi/rpc_server/NaviArpcRequestData.h"

namespace navi {

const std::string NaviArpcRequestData::TYPE_ID = "navi.arpc_request";

NaviArpcRequestData::NaviArpcRequestData(
        const google::protobuf::Message *request,
        google::protobuf::Message *needDeleteRequest,
        const std::string &methodName,
        const std::string &clientIp)
    : Data(TYPE_ID)
    , _request(request)
    , _needDeleteRequest(needDeleteRequest)
    , _methodName(methodName)
    , _clientIp(clientIp)
{
}

NaviArpcRequestData::~NaviArpcRequestData() {
    if (_needDeleteRequest) {
        multi_call::freeProtoMessage(_needDeleteRequest);
    }
}

const google::protobuf::Message *NaviArpcRequestData::getRequest() const {
    if (_request) {
        return _request;
    } else {
        return _needDeleteRequest;
    }
}

NaviArpcRequestType::NaviArpcRequestType()
    : Type(__FILE__, NaviArpcRequestData::TYPE_ID)
{
}

NaviArpcRequestType::~NaviArpcRequestType() {
}

navi::TypeErrorCode
NaviArpcRequestType::serialize(navi::TypeContext &ctx,
                               const navi::DataPtr &data) const {
    auto *requestData = dynamic_cast<NaviArpcRequestData *>(data.get());
    if (!requestData) {
        NAVI_KERNEL_LOG(ERROR, "navi arpc request data is null");
        return navi::TEC_NULL_DATA;
    }
    auto &dataBuffer = ctx.getDataBuffer();
    auto requestPb = requestData->getRequest();
    if (requestPb) {
        dataBuffer.write(true);
        dataBuffer.write(requestPb->GetTypeName());
        dataBuffer.write(requestPb->SerializeAsString());
    } else {
        dataBuffer.write(false);
    }
    dataBuffer.write(requestData->getMethodName());
    dataBuffer.write(requestData->getClientIp());
    dataBuffer.write(requestData->isAiosDebug());
    return navi::TEC_NONE;
}

navi::TypeErrorCode
NaviArpcRequestType::deserialize(navi::TypeContext &ctx,
                                 navi::DataPtr &data) const {
    auto &dataBuffer = ctx.getDataBuffer();
    bool hasRequest = false;
    dataBuffer.read(hasRequest);
    google::protobuf::Message *requestPb = nullptr;
    if (hasRequest) {
        std::string typeName;
        dataBuffer.read(typeName);
        autil::StringView valueStr;
        dataBuffer.read(valueStr);
        auto message = parseProtobufMesasge(typeName, valueStr);
        if (!message) {
            return TEC_FAILED;
        }
        requestPb = message;
    }
    std::string methodName;
    std::string clientIp;
    bool aiosDebug = false;
    dataBuffer.read(methodName);
    dataBuffer.read(clientIp);
    dataBuffer.read(aiosDebug);
    auto requestData = std::make_shared<NaviArpcRequestData>(
        nullptr, requestPb, methodName, clientIp);
    requestData->setAiosDebug(aiosDebug);
    data = requestData;
    return navi::TEC_NONE;
}

google::protobuf::Message *
NaviArpcRequestType::parseProtobufMesasge(const std::string &typeName,
                                          const autil::StringView &valueStr) {
    auto desc = google::protobuf::DescriptorPool::generated_pool()
                    ->FindMessageTypeByName(typeName);
    if (!desc) {
        NAVI_KERNEL_LOG(ERROR, "find protobuf type [%s] descriptor failed",
                        typeName.c_str());
        return nullptr;
    }
    auto message = google::protobuf::MessageFactory::generated_factory()
                       ->GetPrototype(desc)
                       ->New();
    if (!message) {
        NAVI_KERNEL_LOG(ERROR, "new protobuf message failed, type [%s]",
                        typeName.c_str());
        return nullptr;
    }
    if (!message->ParseFromArray(valueStr.data(), valueStr.size())) {
        NAVI_KERNEL_LOG(ERROR, "parse protobuf message failed, type [%s]",
                        typeName.c_str());
        delete message;
        return nullptr;
    }
    return message;
}

REGISTER_TYPE(NaviArpcRequestType);

}
