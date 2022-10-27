#ifndef ISEARCH_DISTINCTCLAUSE_H
#define ISEARCH_DISTINCTCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/FilterClause.h>
#include <ha3/common/ClauseBase.h>
#include <ha3/common/DistinctDescription.h>

BEGIN_HA3_NAMESPACE(common);

#define DEFAULT_DISTINCT_MODULE_NAME ""

class DistinctClause : public ClauseBase
{
public:
    static const uint32_t MAX_DISTINCT_DESC_COUNT = 2;
public:
    DistinctClause();
    ~DistinctClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::vector<DistinctDescription*>& getDistinctDescriptions() const;
    const DistinctDescription* getDistinctDescription(uint32_t pos) const;
    DistinctDescription* getDistinctDescription(uint32_t pos);
    void addDistinctDescription(DistinctDescription *distDescription);
    void clearDistinctDescriptions();
    uint32_t getDistinctDescriptionsCount() const;
private:
    std::vector<DistinctDescription*> _distDescriptions;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_DISTINCTCLAUSE_H
