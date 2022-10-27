#ifndef ISEARCH_DISTINCTCOMPARATOR_H
#define ISEARCH_DISTINCTCOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/DistinctInfo.h>

BEGIN_HA3_NAMESPACE(rank);

// This class may not be used
//
class DistinctComparator : public Comparator
{
public:
    DistinctComparator(matchdoc::Reference<DistinctInfo> *distInfoRef, bool flag =false);
    ~DistinctComparator();
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override;
    std::string getType() const override { return "distinct"; }

private:
    matchdoc::Reference<DistinctInfo> *_distInfoRef;
    bool _flag;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DISTINCTCOMPARATOR_H
