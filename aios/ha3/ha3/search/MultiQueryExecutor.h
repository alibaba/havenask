#ifndef ISEARCH_MULTIQUERYEXECUTOR_H
#define ISEARCH_MULTIQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class MultiQueryExecutor : public QueryExecutor
{
public:
    typedef std::vector<QueryExecutor *> QueryExecutorVector;

public:
    MultiQueryExecutor();
    virtual ~MultiQueryExecutor();
public:
    const std::string getName() const override;
    void reset() override;
    void moveToEnd() override;
    void setCurrSub(docid_t docid) override;
    uint32_t getSeekDocCount() override;
    virtual void addQueryExecutors(const std::vector<QueryExecutor*> &queryExecutors) = 0;

    inline QueryExecutor *getQueryExecutor(int idx) {
        return _queryExecutors[idx];
    }

    inline void clear() {
        _queryExecutors.clear();
    }

    inline const QueryExecutor *getQueryExecutor(int idx) const {
        return _queryExecutors[idx];
    }

    inline int32_t getQueryExecutorCount() const {
        return _queryExecutors.size();
    }

    inline const QueryExecutorVector& getQueryExecutors() const {
        return _queryExecutors;
    }

    inline QueryExecutorVector& getQueryExecutors() {
        return _queryExecutors;
    }

public:
    // for test.
    inline void addQueryExecutor(QueryExecutor *queryExecutor) {
        _queryExecutors.push_back(queryExecutor);
    }
    
    template <class QueryExcutorType>
    static std::string convertToString(const std::vector<QueryExcutorType*> &queryExecutors);
    std::string toString() const override;

protected:
    QueryExecutorVector _queryExecutors;
private:
    HA3_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

template <class QueryExcutorType>
std::string MultiQueryExecutor::convertToString(const std::vector<QueryExcutorType*> &queryExecutors) {
    std::string queryStr = "(";
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        queryStr += queryExecutors[i]->toString();
        queryStr += queryExecutors.size() - 1 == i ? "" : ",";
    }
    queryStr += ")";
    return queryStr;
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MULTIQUERYEXECUTOR_H
