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
#include "navi/rpc_server/NaviArpcResponseData.h"

namespace navi {

const std::string NaviArpcResponseData::TYPE_ID = "navi.arpc_response";

NaviArpcResponseData::NaviArpcResponseData(google::protobuf::Message *response)
    : Data(TYPE_ID)
    , _response(response)
{
}

NaviArpcResponseData::~NaviArpcResponseData() {
    multi_call::freeProtoMessage(_response);
}

google::protobuf::Message *NaviArpcResponseData::stealResponse() {
    auto tmp = _response;
    _response = nullptr;
    return tmp;
}

NaviArpcResponseType::NaviArpcResponseType()
    : Type(__FILE__, NaviArpcResponseData::TYPE_ID)
{
}

NaviArpcResponseType::~NaviArpcResponseType() {
}

REGISTER_TYPE(NaviArpcResponseType);

}

