#include <ha3/common/OptimizerClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, OptimizerClause);

OptimizerClause::OptimizerClause() { 
}

OptimizerClause::~OptimizerClause() { 
}

void OptimizerClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_optimizeNames);
    dataBuffer.write(_optimizeOptions);
}

void OptimizerClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_optimizeNames);
    dataBuffer.read(_optimizeOptions);
}

std::string OptimizerClause::toString() const {
    std::string optimizeStr;
    size_t optimizerNum = std::min(_optimizeNames.size(), _optimizeOptions.size());
    for (size_t i = 0; i < optimizerNum; i++) {
        optimizeStr.append(_optimizeNames[i]);
        optimizeStr.append(":");
        optimizeStr.append(_optimizeOptions[i]);
        optimizeStr.append(";");
    }
    return optimizeStr;
}

END_HA3_NAMESPACE(common);

