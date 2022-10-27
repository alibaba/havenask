#ifndef ISEARCH_PKFILTERCLAUSE_H
#define ISEARCH_PKFILTERCLAUSE_H

#include <string>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class PKFilterClause : public ClauseBase
{
public:
    PKFilterClause();
    ~PKFilterClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PKFilterClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_PKFILTERCLAUSE_H
