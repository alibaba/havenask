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
#include "build_service/io/Input.h"
#include "build_service/util/Log.h"

namespace build_service { namespace io {

class InputCreator
{
public:
    InputCreator() {}
    virtual ~InputCreator() {}

private:
    InputCreator(const InputCreator&);
    InputCreator& operator=(const InputCreator&);

public:
    virtual bool init(const config::TaskInputConfig& inputConfig) = 0;
    virtual InputPtr create(const KeyValueMap& params) = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(InputCreator);

}} // namespace build_service::io
