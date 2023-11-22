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
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace indexlibv2::config {
class IIndexConfig;
}

namespace build_service { namespace proto {

/** jsonized format: **
 {
     "indexs" :
     [
         {
             "name": "index1",
             "type": "NUMBER",
             "operation_id" : 1
         },
         ...
     ],
     "attributes" :
     [
         {
             "name": "new_price",
             "type": "int8",
             "operation_id" : 2
         }
     ]
 }
 */
class EffectiveIndexInfo : public autil::legacy::Jsonizable
{
public:
    EffectiveIndexInfo();
    ~EffectiveIndexInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void addIndex(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

private:
    std::vector<std::map<std::string, std::string>> _indexs;
    std::vector<std::map<std::string, std::string>> _attributes;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EffectiveIndexInfo);

}} // namespace build_service::proto
