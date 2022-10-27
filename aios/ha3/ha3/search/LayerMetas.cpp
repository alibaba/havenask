#include <ha3/search/LayerMetas.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);

DocIdRangeMeta::DocIdRangeMeta() {
    nextBegin = 0;
    end = 0;
    quota = 0;
    begin = 0;
}

DocIdRangeMeta::DocIdRangeMeta(docid_t begin, docid_t end, uint32_t quota) {
    this->begin = begin;
    this->end = end;
    this->nextBegin = begin;
    this->quota = quota;
}

DocIdRangeMeta::DocIdRangeMeta(const DocIdRange& docIdRange, uint32_t quota) {
    begin = docIdRange.first;
    end = docIdRange.second;
    nextBegin = begin;
    this->quota = quota;
}

LayerMeta::LayerMeta(autil::mem_pool::Pool *pool)
    : autil::mem_pool::PoolVector<DocIdRangeMeta>(pool)
{
    quota = 0;
    maxQuota = std::numeric_limits<uint32_t>::max();
    quotaMode = QM_PER_DOC;
    needAggregate = true;
    quotaType = QT_PROPOTION;
}

std::string LayerMeta::toString() const {
    std::stringstream ss;
    ss << "(quota: " << quota
       << " maxQuota: " << maxQuota
       << " quotaMode: " << quotaMode
       << " needAggregate: " << needAggregate
       << " quotaType: " << quotaType
       << ") ";
    for (const_iterator it = begin(); it != end(); it++) {
        ss << *it << ";";
    }
    return ss.str();
}

void LayerMeta::initRangeString() {
    std::stringstream ss;
    for (const_iterator it = begin(); it != end(); it++) {
        ss << *it << ";";
    }
    this->rangeString = ss.str();
}

std::string LayerMeta::getRangeString() const {
    return rangeString;
}

std::ostream& operator << (std::ostream &os, const DocIdRangeMeta &range) {
    os << "begin: " << range.begin
       << " end: " << range.end
       << " nextBegin: " << range.nextBegin
       << " quota: " << range.quota;
    return os;
}

END_HA3_NAMESPACE(search);

