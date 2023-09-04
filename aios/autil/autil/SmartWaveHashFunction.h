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

#include <map>
#include <unordered_map>

#include "autil/DefaultHashFunction.h"
#include "autil/HashFunctionBase.h"

/*

功能：针对配置的数据，按顺序将不同的数据映射到不同的列上。开发该hash函数的初衷是减少智能波次的大仓倾斜问题。通过提前将大仓hash到不同的列上，避免大仓被hash到同一列，引发严重的数据倾斜问题。不在配置名单中的数据仍然按照默认的hash方式

配置方式：
        "hash_mode": {
            "hash_field": "warehouse_id",
            "hash_function": "SMARTWAVE_HASH",
            "hash_params": {
                "partition_count": "32", //分列的个数
                "bigdata_values": "8,4000038,54,427,1588,395,1204,38,583,427,771,1167,551" //需要hash到不同列上的数据
            }
        },

注意：这里配置需要离线在线都要修改。

对应修改点：iquan/iquan_core/src/main/java/com/taobao/search/iquan/core/api/schema/HashType.java中需要加入新增的hash名，在线部分加载时会校验

*/
namespace autil {

class SmartWaveHashFunction : public HashFunctionBase {
public:
    SmartWaveHashFunction(const std::string &hashFunction,
                          const std::map<std::string, std::string> &params,
                          uint32_t partitionCount)
        : HashFunctionBase(hashFunction, partitionCount), _params(params) {}
    virtual ~SmartWaveHashFunction() {}

public:
    bool init() override;
    uint32_t getHashId(const std::string &str) const override;

private:
    std::map<std::string, std::string> _params;
    std::unordered_map<std::string, uint32_t> _bigDataMap;
    uint32_t _partNum;
    DefaultHashFunctionPtr _defaultHashFuncPtr;
};

typedef std::shared_ptr<SmartWaveHashFunction> SmartWaveHashFunctionPtr;

} // namespace autil
