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
#include "suez/turing/common/GraphConfig.h"

#include <cstdint>
#include <iosfwd>
#include <stdint.h>

using namespace std;

namespace suez {
namespace turing {

void CudaGraphConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("enable", _enable, _enable);
    json.Jsonize("output_names", _outputNames, _outputNames);
    json.Jsonize("batchs", _batchs, _batchs);
    json.Jsonize("output_on_cpu", _outputOnCPU, _outputOnCPU);
    json.Jsonize("enable_xla", _enableCudaGraphXla, _enableCudaGraphXla);
}

bool CudaGraphConfig::operator==(const CudaGraphConfig &other) const {
    if (this == &other) {
        return true;
    }
    return _enable == other._enable && _outputNames == other._outputNames && _batchs == other._batchs &&
           _outputOnCPU == other._outputOnCPU && _enableCudaGraphXla == other._enableCudaGraphXla;
}

void GraphConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("graph_name", _graphName, _graphName);
    json.Jsonize("graph_file", _graphFile, _graphFile);
    json.Jsonize("restore_node_map", _restoreNodeMap, _restoreNodeMap);
    json.Jsonize("cuda_graph_config", _cudaGraphConfig, _cudaGraphConfig);
}

bool GraphConfig::operator==(const GraphConfig &other) const {
    if (this == &other) {
        return true;
    }
    return _graphName == other._graphName && _graphFile == other._graphFile &&
           _restoreNodeMap == other._restoreNodeMap && _cudaGraphConfig == other._cudaGraphConfig;
}

} // namespace turing
} // namespace suez
