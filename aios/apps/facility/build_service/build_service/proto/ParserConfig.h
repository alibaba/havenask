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
#ifndef ISEARCH_BS_PARSERCONFIG_H
#define ISEARCH_BS_PARSERCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

class ParserConfig : public autil::legacy::Jsonizable
{
public:
    ParserConfig() {}
    ~ParserConfig() {}

public:
    ParserConfig(const ParserConfig& other) : type(other.type), parameters(other.parameters) {}

    ParserConfig& operator=(const ParserConfig& other)
    {
        type = other.type;
        parameters = other.parameters;
        return *this;
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("type", type, type);
        json.Jsonize("parameters", parameters, parameters);
    }
    bool operator==(const ParserConfig& other) const { return type == other.type && (parameters == other.parameters); }
    bool operator!=(const ParserConfig& other) const { return !(operator==(other)); }

public:
    std::string type;
    KeyValueMap parameters;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ParserConfig);

}} // namespace build_service::proto

#endif // ISEARCH_BS_PARSERCONFIG_H
