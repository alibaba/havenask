#include <ha3/common/LayerDescription.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, LayerDescription);

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

END_HA3_NAMESPACE(common);

