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

#include "build_service/common_define.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace io {

class Input
{
public:
    enum ErrorCode { EC_ERROR, EC_OK, EC_EOF };

public:
    Input(const config::TaskInputConfig& inputConfig) : _inputConfig(inputConfig) {}
    virtual ~Input() {}

private:
    Input(const Input&);
    Input& operator=(const Input&);

public:
    virtual bool init(const KeyValueMap& params) = 0;
    virtual std::string getType() const = 0;
    virtual ErrorCode read(autil::legacy::Any& any) = 0;

protected:
    config::TaskInputConfig _inputConfig;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Input);

}} // namespace build_service::io
