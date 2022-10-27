#include <ha3/common/AttributeClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AttributeClause);

AttributeClause::AttributeClause() { 
}

AttributeClause::~AttributeClause() { 
}

void AttributeClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_attributeNames);
}

void AttributeClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_attributeNames);
}

std::string AttributeClause::toString() const {
    std::string attrClauseStr;
    std::set<std::string>::const_iterator iter 
        =  _attributeNames.begin();
    for (; iter != _attributeNames.end(); iter++) {
        attrClauseStr.append(*iter);
        attrClauseStr.append("|");
    }
    return attrClauseStr;
}

END_HA3_NAMESPACE(common);

