#include <ha3/common/FinalSortClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, FinalSortClause);

const std::string FinalSortClause::DEFAULT_SORTER = "DEFAULT";

FinalSortClause::FinalSortClause() { 
}

FinalSortClause::~FinalSortClause() { 
}

void FinalSortClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_sortName);
    dataBuffer.write(_params);
}

void FinalSortClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_sortName);
    dataBuffer.read(_params);
}

std::string FinalSortClause::toString() const {
    std::string finalSortStr;
    finalSortStr.append(_sortName);
    finalSortStr.append("[");
    for (KeyValueMap::const_iterator it = _params.begin(); 
         it != _params.end(); ++it)
    {
        finalSortStr.append(it->first);
        finalSortStr.append(":");
        finalSortStr.append(it->second);
        finalSortStr.append(",");
    }
    finalSortStr.append("]");
    return finalSortStr;
}

END_HA3_NAMESPACE(common);

