#include <ha3/common/AndQuery.h>
#include <sstream>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AndQuery);

AndQuery::AndQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

AndQuery::AndQuery(const AndQuery &other)
    : Query(other)
{
}

AndQuery::~AndQuery() {
}

bool AndQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const AndQuery&>(query)._children;

    if (_children.size() != children2.size()) {
        return false;
    }
    QueryVector::const_iterator it1 = _children.begin();
    QueryVector::const_iterator it2 = children2.begin();
    for (; it1 != _children.end(); it1++, it2++)
    {
        if (!( *(*it1) == *(*it2) )) {
            return false;
        }
    }
    return true;
}

void AndQuery::accept(QueryVisitor *visitor) const {
    visitor->visitAndQuery(this);
}

void AndQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitAndQuery(this);
}

Query *AndQuery::clone() const {
    return new AndQuery(*this);
}

END_HA3_NAMESPACE(common);
