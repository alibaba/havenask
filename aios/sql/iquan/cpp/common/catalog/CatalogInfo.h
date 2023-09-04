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
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/ComputeNodeModel.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/TvfFunctionModel.h"

namespace iquan {

class CatalogInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("catalog_name", catalogName);
        json.Jsonize("version", version);
        json.Jsonize("compute_nodes", computeNodeModels, computeNodeModels);
        json.Jsonize("tables", tableModels, tableModels);
        json.Jsonize("layer_tables", layerTableModels, layerTableModels);
        json.Jsonize("functions", functionModels, functionModels);
        json.Jsonize("tvf_functions", tvfFunctionModels, tvfFunctionModels);
    }

    bool isValid() const {
        return (!catalogName.empty())
               && (tableModels.isValid() || functionModels.isValid()
                   || tvfFunctionModels.isValid());
    }

public:
    std::string catalogName;
    int64_t version = -1;
    ComputeNodeModels computeNodeModels;
    TableModels tableModels;
    LayerTableModels layerTableModels;
    FunctionModels functionModels;
    TvfModels tvfFunctionModels;
};

} // namespace iquan
