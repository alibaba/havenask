#ifndef ISEARCH_LAYERMETAS_H
#define ISEARCH_LAYERMETAS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <sstream>
#include <autil/mem_pool/PoolVector.h>

BEGIN_HA3_NAMESPACE(search);

struct DocIdRangeMeta {
    DocIdRangeMeta();
    DocIdRangeMeta(docid_t begin, docid_t end, uint32_t quota = 0);
    DocIdRangeMeta(const DocIdRange& docIdRange, uint32_t quota = 0);

    bool operator != (const DocIdRangeMeta &other) const {
        return !(*this == other);
    }
    bool operator == (const DocIdRangeMeta &other) const {
        return begin == other.begin
            && end == other.end
            && nextBegin == other.nextBegin
            && quota == other.quota;
    }
    bool operator==(const DocIdRangeMeta& meta) {
        return begin == meta.begin
            && end == meta.end
            && nextBegin == meta.nextBegin
            && quota == meta.quota;
    }

    docid_t nextBegin;
    docid_t end;
    uint32_t quota;
    docid_t begin;
};

struct DocIdRangeMetaCompare {
    bool operator() (const DocIdRangeMeta& i,
                     const DocIdRangeMeta& j) {
        return i.begin < j.begin;
    }
};

///////////////////////////////////////////////////////////
std::ostream& operator << (std::ostream &os, const DocIdRangeMeta &range);

class LayerMeta : public autil::mem_pool::PoolVector<DocIdRangeMeta> {
public:
    LayerMeta(autil::mem_pool::Pool *pool);
    void initRangeString();
    std::string toString() const;
    std::string getRangeString() const;
public:    
    uint32_t quota;
    uint32_t maxQuota;
    QuotaMode quotaMode;
    bool needAggregate;
    QuotaType quotaType;
private:
    std::string rangeString;
};

typedef autil::mem_pool::PoolVector<LayerMeta> LayerMetas;

HA3_TYPEDEF_PTR(LayerMetas);

HA3_TYPEDEF_PTR(LayerMeta);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERMETAS_H
