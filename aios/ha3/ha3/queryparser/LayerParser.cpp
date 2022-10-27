#include <ha3/queryparser/LayerParser.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, LayerParser);

LayerParser::LayerParser() { 
}

LayerParser::~LayerParser() { 
}

common::QueryLayerClause *LayerParser::createLayerClause() {
    return new common::QueryLayerClause;
}

common::LayerDescription *LayerParser::createLayerDescription() {
    return new common::LayerDescription;
}

void LayerParser::setQuota(common::LayerDescription *layerDesc, const std::string &quotaStr) {
    uint32_t quota = 0;
    const std::vector<std::string>& strVec = autil::StringUtil::split(quotaStr, "#");
    if(strVec.size() > 0) {
        if (strVec[0] == "UNLIMITED") {
            quota = std::numeric_limits<uint32_t>::max();
        } else {
            if (!autil::StringUtil::fromString(strVec[0].c_str(), quota)) {
                HA3_LOG(WARN, "Invalid quota string: [%s].", strVec[0].c_str());
                return;
            }
        }
        layerDesc->setQuota(quota);
        if(2 == strVec.size()) {
            uint32_t quotaType = 0;
            if (!autil::StringUtil::fromString(strVec[1].c_str(), quotaType)) {
                HA3_LOG(WARN, "Invalid quota string: [%s], use default 0.", strVec[1].c_str());
                quotaType = 0;
            }
            if(quotaType >= QT_UNKNOW) {
                HA3_LOG(WARN, "Invalid quota string: [%s], use default 0.", strVec[1].c_str());
                quotaType = 0;
            }
            layerDesc->setQuotaType((QuotaType)quotaType);
        }
    }
}


END_HA3_NAMESPACE(queryparser);

