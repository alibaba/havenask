#ifndef ISEARCH_COMPARATOR_H
#define ISEARCH_COMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/MatchDocs.h>

BEGIN_HA3_NAMESPACE(rank);

class Comparator
{
public:
    Comparator();
    virtual ~Comparator();
public:
    virtual bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const  = 0;
    virtual std::string getType() const  { return "base"; }
private:
    HA3_LOG_DECLARE();
};

class MatchDocComp {
public:
    MatchDocComp(const Comparator *comp) {
        _comp = comp;
    }
public:
    bool operator() (matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) {
        return _comp->compare(rht, lft);
    }
private:
    const Comparator *_comp;
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_COMPARATOR_H
