#include <ha3/common/OrQuery.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>
#include <sstream>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, OrQuery);

OrQuery::OrQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

OrQuery::OrQuery(const OrQuery &other)
    : Query(other)
{
}

OrQuery::~OrQuery() {
}

bool OrQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const OrQuery&>(query)._children;

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

void OrQuery::accept(QueryVisitor *visitor) const {
    visitor->visitOrQuery(this);
}

void OrQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitOrQuery(this);
}

Query *OrQuery::clone() const {
    return new OrQuery(*this);
}

END_HA3_NAMESPACE(common);
