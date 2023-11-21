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

#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class DocReclaimSource : public autil::legacy::Jsonizable
{
public:
    DocReclaimSource();
    ~DocReclaimSource();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string swiftRoot;
    std::string topicName;
    std::string clientConfigStr;
    std::string readerConfigStr;
    int64_t msgTTLInSeconds;

public:
    static std::string DOC_RECLAIM_SOURCE;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocReclaimSource);

}} // namespace build_service::config
