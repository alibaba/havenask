#include <ha3/common/QueryLayerClause.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, QueryLayerClause);

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

END_HA3_NAMESPACE(common);

