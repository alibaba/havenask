#ifndef ISEARCH_QUERYCLAUSE_H
#define ISEARCH_QUERYCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Query.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class QueryClause : public ClauseBase
{
public:
    QueryClause();
    QueryClause(Query* query);
    ~QueryClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRootQuery(Query *query, uint32_t layer = 0);
    Query *getRootQuery(uint32_t layer = 0) const;
    void insertQuery(Query *query, int32_t layer);
    uint32_t getQueryCount() const {
        return _rootQuerys.size();
    }
private:
    std::vector<Query*> _rootQuerys;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<QueryClause> QueryClausePtr;

typedef QueryClause AuxQueryClause;
typedef std::shared_ptr<AuxQueryClause> AuxQueryClausePtr;


END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYCLAUSE_H
