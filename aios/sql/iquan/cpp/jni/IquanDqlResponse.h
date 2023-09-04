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

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/SqlPlan.h"

namespace iquan {
class ResponseHeader : public autil::legacy::Jsonizable {
public:
    ResponseHeader()
        : errorCode(0) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("error_code", errorCode);
        json.Jsonize("error_message", errorMsg);
    }

    static Status check(const std::string &response) {
        ResponseHeader header;
        IQUAN_ENSURE_FUNC(Utils::fromJson(header, response));
        if (header.errorCode != IQUAN_OK) {
            return Status(header.errorCode, header.errorMsg);
        }
        return Status::OK();
    }

public:
    int errorCode;
    std::string errorMsg;
};

class IquanDqlResponse : public ResponseHeader {
public:
    IquanDqlResponse() = default;
    IquanDqlResponse(const SqlPlan &plan)
        : sqlPlan(plan) {}
    ~IquanDqlResponse() {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        ResponseHeader::Jsonize(json);
        if (errorCode == IQUAN_OK) {
            json.Jsonize("result", sqlPlan);
        }
    }

public:
    SqlPlan sqlPlan;
};

class IquanDqlResponseWrapper : public ResponseHeader {
public:
    IquanDqlResponseWrapper() = default;

    bool convert(const IquanDqlResponse &response, const DynamicParams &dynamicParams) {
        errorCode = response.errorCode;
        errorMsg = response.errorMsg;
        return sqlPlanWrapper.convert(response.sqlPlan, dynamicParams);
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        ResponseHeader::Jsonize(json);
        if (errorCode == IQUAN_OK) {
            json.Jsonize("result", sqlPlanWrapper);
        }
    }

public:
    SqlPlanWrapper sqlPlanWrapper;
};

} // namespace iquan
