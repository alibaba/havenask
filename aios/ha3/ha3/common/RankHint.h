#ifndef ISEARCH_RANKHINT_H
#define ISEARCH_RANKHINT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>

BEGIN_HA3_NAMESPACE(common);

struct RankHint {
public:
    RankHint();
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
public:
    std::string sortField;
    SortPattern sortPattern;
};

HA3_TYPEDEF_PTR(RankHint);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANKHINT_H
