#ifndef ISEARCH_QUERY_H
#define ISEARCH_QUERY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <assert.h>
#include <ostream>
#include <ha3/util/Serialize.h>

BEGIN_HA3_NAMESPACE(common);

class QueryVisitor;
class ModifyQueryVisitor;
class Query;
HA3_TYPEDEF_PTR(Query);

class Query
{
public:
    typedef std::vector<QueryPtr> QueryVector;
public:
    Query();
    Query(const Query &other);
    virtual ~Query();
public:
    virtual bool operator == (const Query &query) const = 0;
    virtual void accept(QueryVisitor *visitor) const = 0;
    virtual void accept(ModifyQueryVisitor *visitor) = 0;
    virtual Query *clone() const = 0;

    virtual void addQuery(QueryPtr queryPtr) {
        _children.push_back(queryPtr);
    }
    const std::vector<QueryPtr>* getChildQuery() const {
        return &_children;
    }
    std::vector<QueryPtr>* getChildQuery() {
        return &_children;
    }
    void setQueryRestrictor(bool restrictorFlag) {
        _queryRestrictor = restrictorFlag;
    }
    bool hasQueryRestrictor() const {
        return _queryRestrictor;
    }
    virtual std::string getQueryName() const = 0;
    virtual std::string toString() const;

    virtual void serialize(autil::DataBuffer &dataBuffer) const;
    virtual void deserialize(autil::DataBuffer &dataBuffer);

    virtual QueryType getType() const = 0;
    static Query* createQuery(QueryType type);
    const std::string& getQueryLabel() const { return _queryLabel; }
    void setQueryLabel(const std::string &label) { _queryLabel = label; }
    MatchDataLevel getMatchDataLevel() const { return _matchDataLevel; }
    void setMatchDataLevel(MatchDataLevel level) { _matchDataLevel = level; }
    virtual void setQueryLabelWithDefaultLevel(const std::string &label) = 0;
protected:
    void setQueryLabelBinary(const std::string &label);
    void setQueryLabelTerm(const std::string &label);
    void serializeMDLandQL(autil::DataBuffer &dataBuffer) const;
    void deserializeMDLandQL(autil::DataBuffer &dataBuffer);
protected:
    QueryVector _children;
    bool _queryRestrictor;
    MatchDataLevel _matchDataLevel;
    std::string _queryLabel;
private:
    HA3_LOG_DECLARE();
};

std::ostream& operator << (std::ostream& out, const Query& query);

END_HA3_NAMESPACE(common);

AUTIL_BEGIN_NAMESPACE(autil);

template<>
inline void DataBuffer::write<HA3_NS(common)::Query>(HA3_NS(common)::Query const * const &p) {
    bool isNull = p;
    write(isNull);
    if (isNull) {
        QueryType type = p->getType();
        write(type);
        write(*p);
    }
}

template<>
inline void DataBuffer::read<HA3_NS(common)::Query>(HA3_NS(common)::Query* &p) {
    bool isNull;
    read(isNull);
    if (isNull) {
        QueryType type;
        read(type);
        p = HA3_NS(common)::Query::createQuery(type);
        if (p) {
            read(*p);
        }
    } else {
        p = NULL;
    }
}

AUTIL_END_NAMESPACE(autil);

#endif //ISEARCH_QUERY_H
