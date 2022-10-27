#include <sstream>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AndNotQuery);


AndNotQuery::AndNotQuery(const std::string& label) {
    setQueryLabelBinary(label);
}

AndNotQuery::~AndNotQuery() {
}


bool AndNotQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const AndNotQuery&>(query)._children;

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

void AndNotQuery::accept(QueryVisitor *visitor) const  {
    visitor->visitAndNotQuery(this);
}

void AndNotQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitAndNotQuery(this);
}

Query *AndNotQuery::clone() const {
    return new AndNotQuery(*this);
}

END_HA3_NAMESPACE(common);
