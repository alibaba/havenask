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

namespace navi {

class NaviArpcResponseData : public Data
{
public:
    NaviArpcResponseData(google::protobuf::Message *response);
    ~NaviArpcResponseData();
    NaviArpcResponseData(const NaviArpcResponseData &) = delete;
    NaviArpcResponseData &operator=(const NaviArpcResponseData &) = delete;
public:
    static const std::string TYPE_ID;
public:
    google::protobuf::Message *stealResponse();
private:
    google::protobuf::Message *_response;
};

class NaviArpcResponseType : public Type {
public:
    NaviArpcResponseType();
    ~NaviArpcResponseType();
};

}

