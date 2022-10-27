#ifndef ISEARCH_GRADECOMPARATOR_H
#define ISEARCH_GRADECOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/DistinctInfo.h>
#include <matchdoc/Reference.h>

BEGIN_HA3_NAMESPACE(rank);

// This class may not be used
//
class GradeComparator : public Comparator
{
public:
    GradeComparator(matchdoc::Reference<DistinctInfo> *distInfoRef, bool flag = false);
    ~GradeComparator();
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override;

    std::string getType() const  override { return "grade"; }

private:
    matchdoc::Reference<DistinctInfo> *_distInfoRef;
    bool _flag;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_GRADECOMPARATOR_H
