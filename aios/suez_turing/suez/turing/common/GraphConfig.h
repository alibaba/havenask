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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

struct CudaGraphConfig : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const CudaGraphConfig &other) const;
    bool operator!=(const CudaGraphConfig &other) const { return !(*this == other); }

public:
    bool _enable = false;
    std::vector<std::string> _outputNames;
    std::vector<std::pair<int32_t, int32_t>> _batchs;
    bool _outputOnCPU = false;
    bool _enableCudaGraphXla = false;
};

class GraphConfig : public autil::legacy::Jsonizable {
public:
    using RestoreNodeMap = std::map<std::string, std::pair<std::string, std::string>>;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const GraphConfig &other) const;
    bool operator!=(const GraphConfig &other) const { return !(*this == other); }

public:
    const std::string &name() const { return _graphName; }
    const std::string &graphFile() const { return _graphFile; }
    const RestoreNodeMap &restoreNodeMap() const { return _restoreNodeMap; }
    const CudaGraphConfig &cudaGraphConfig() const { return _cudaGraphConfig; }
    bool cudaGraphEnabled() const { return _cudaGraphConfig._enable; }

public:
    // SessionOptions & session_count或许也应该放进去
    std::string _graphName;
    std::string _graphFile;
    RestoreNodeMap _restoreNodeMap;
    CudaGraphConfig _cudaGraphConfig;
};
using GraphConfigMap = std::map<std::string, GraphConfig>;

} // namespace turing
} // namespace suez
