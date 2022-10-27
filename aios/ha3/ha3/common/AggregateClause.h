#ifndef _AGGREGATECLAUSE_H
#define _AGGREGATECLAUSE_H

#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AggregateDescription.h>
#include <ha3/util/Serialize.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

typedef std::vector<AggregateDescription*> AggregateDescriptions;

class AggregateClause : public ClauseBase
{
public:
    AggregateClause();
    ~AggregateClause();
    AggregateClause(const AggregateClause &aggregateClause);

public:
    HA3_SERIALIZE_DECLARE(override);
    std::string toString() const override;
public:
    void addAggDescription(AggregateDescription *aggDescription);
    bool removeAggDescription(AggregateDescription *aggDescription);
    void clearAggDescriptions();
    const AggregateDescriptions& getAggDescriptions() const;
private:
    AggregateDescriptions _aggDescriptions;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<AggregateClause> AggregateClausePtr;

END_HA3_NAMESPACE(common);

#endif
