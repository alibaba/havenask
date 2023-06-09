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
#include "ha3/common/LayerDescription.h"

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, LayerDescription);

const std::string LayerDescription::INFINITE = "INFINITE";

LayerDescription::LayerDescription() {
    _quota = 0;
    _quotaMode = QM_PER_DOC;
    _quotaType = QT_PROPOTION;
}

LayerDescription::~LayerDescription() {
    resetLayerRange();
}

void LayerDescription::resetLayerRange() {
    for (std::vector<LayerKeyRange*>::iterator it = _ranges.begin();
         it != _ranges.end(); ++it) {
        delete *it;
        *it = NULL;
    }
    _ranges.clear();
}

void LayerDescription::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_ranges);
    dataBuffer.write(_quota);
    int32_t typeAndMode = (_quotaType << 16) | _quotaMode;
    dataBuffer.write(typeAndMode);
}

void LayerDescription::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_ranges);
    dataBuffer.read(_quota);
    int32_t typeAndMode;
    dataBuffer.read(typeAndMode);
    _quotaType = (QuotaType)(typeAndMode >> 16);
    _quotaMode = (QuotaMode)(typeAndMode & 0x0000FFFF);
}

std::string LayerKeyRange::toString() const {
    std::string layerKeyRangeStr;
    layerKeyRangeStr.append(attrName);
    layerKeyRangeStr.append("[");
    for (std::vector<std::string>::const_iterator it = values.begin();
         it != values.end(); ++it)
    {
        layerKeyRangeStr.append(*it);
        layerKeyRangeStr.append(",");
    }

    for (std::vector<LayerAttrRange>::const_iterator it = ranges.begin();
         it != ranges.end(); ++it)
    {
        layerKeyRangeStr.append(it->from);
        layerKeyRangeStr.append(":");
        layerKeyRangeStr.append(it->to);
        layerKeyRangeStr.append(",");
    }
    layerKeyRangeStr.append("]");
    return layerKeyRangeStr;
}

std::string LayerDescription::toString() const {
    std::string layerDesStr;
    layerDesStr.append("keyRanges=");
    for (std::vector<LayerKeyRange*>::const_iterator it = _ranges.begin();
         it != _ranges.end(); ++it)
    {
        layerDesStr.append((*it)->toString());
        layerDesStr.append(";");
    }
    layerDesStr.append(",");
    layerDesStr.append("quota=");
    layerDesStr.append(autil::StringUtil::toString(_quota));
    layerDesStr.append(",");
    layerDesStr.append("quotaMode=");
    layerDesStr.append(autil::StringUtil::toString(int32_t(_quotaMode)));
    return layerDesStr;
}

} // namespace common
} // namespace isearch
