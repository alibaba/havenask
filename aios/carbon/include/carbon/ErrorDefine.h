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
#ifndef CARBON_ERRORDEFINE_H
#define CARBON_ERRORDEFINE_H

#include "CommonDefine.h"
#include "autil/legacy/jsonizable.h"

namespace carbon {

enum ErrorCode {
    EC_NONE = 0,
    EC_SERIALIZE_FAILED,
    EC_DESERIALIZE_FAILED,
    EC_GROUP_ALREADY_EXIST,
    EC_GROUP_NOT_EXIST,
    EC_PERSIST_GROUP,
    EC_SERIALIZE_GROUP,
    EC_CARBON_MASTER_NOT_READY,
    EC_NOT_SUPPORT_METHOD,
    EC_JSON_NODE_ALREADY_EXIST,
    EC_JSON_NODE_NOT_EXIST,
    EC_JSON_INVALID,
    EC_ADJUST_ARGS_FAILED_FOR_DEBUG,
    EC_FIELD_NOT_EXIST,
    EC_NODE_NOT_FOUND,
    EC_PERSIST_FAILED,
    EC_RELEASE_SLOTS_FAILED,
};

struct ErrorInfo : public autil::legacy::Jsonizable{
    ErrorCode errorCode;
    std::string errorMsg;

public:
    ErrorInfo() {
        errorCode = EC_NONE;
    }
    
    JSONIZE() {
        JSON_FIELD(errorCode);
        JSON_FIELD(errorMsg);
    }
};

}

#endif
