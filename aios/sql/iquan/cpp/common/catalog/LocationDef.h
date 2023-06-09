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

namespace iquan {

class LocationDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("partition_cnt", partitionCnt, partitionCnt);
        json.Jsonize("table_group_name", tableGroupName, tableGroupName);
        json.Jsonize("service_desc", serviceDesc, serviceDesc);
    }

    bool isValid() const { return true; }

public:
    int partitionCnt = 0;
    std::string tableGroupName;
    std::string serviceDesc;
};

} // namespace iquan
