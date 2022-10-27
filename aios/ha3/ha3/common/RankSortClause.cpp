#include <ha3/common/RankSortClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RankSortClause);

RankSortClause::RankSortClause() {
}

RankSortClause::~RankSortClause() {
    for (size_t i = 0; i < _descs.size(); ++i) {
        delete _descs[i];
    }
    _descs.clear();
}

void RankSortClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_descs);
}

void RankSortClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_descs);
}

std::string RankSortClause::toString() const {
    std::string rankSortStr;
    for (size_t i = 0; i < _descs.size(); i++) {
        assert(_descs[i]);
        rankSortStr.append(_descs[i]->toString());
        rankSortStr.append(";");
    }
    return rankSortStr;
}

END_HA3_NAMESPACE(common);

