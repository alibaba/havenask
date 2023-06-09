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
#include "ha3/common/QueryLayerClause.h"

#include <stdint.h>
#include <cstddef>

#include "autil/DataBuffer.h"

#include "ha3/common/LayerDescription.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryLayerClause);

QueryLayerClause::QueryLayerClause() { 
}

QueryLayerClause::~QueryLayerClause() { 
    for (std::vector<LayerDescription*>::iterator it = _layerDescriptions.begin();
         it != _layerDescriptions.end(); ++it)
    {
        delete *it;
        *it = NULL;
    }
    _layerDescriptions.clear();
}

LayerDescription *QueryLayerClause::getLayerDescription(size_t pos) const {
    if (pos >= (uint32_t)_layerDescriptions.size()) {
        return NULL;
    }
    return _layerDescriptions[pos];
}

void QueryLayerClause::addLayerDescription(LayerDescription *layerDesc) {
    _layerDescriptions.push_back(layerDesc);
}

void QueryLayerClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_layerDescriptions);
}

void QueryLayerClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_layerDescriptions);
}

std::string QueryLayerClause::toString() const {
    string layerClauseStr;
    for (vector<LayerDescription*>::const_iterator it = _layerDescriptions.begin(); 
         it != _layerDescriptions.end(); ++it)
    {
        layerClauseStr.append("{");
        layerClauseStr.append((*it)->toString());
        layerClauseStr.append("}");
    }
    return layerClauseStr;
}

} // namespace common
} // namespace isearch

