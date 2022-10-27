#ifndef ISEARCH_REFERENCECOMPARATOR_H
#define ISEARCH_REFERENCECOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <stdio.h>

BEGIN_HA3_NAMESPACE(rank);

template<typename T>
class ReferenceComparator : public Comparator
{
public:
    ReferenceComparator(const matchdoc::Reference<T> *variableReference, bool flag = false) {
        _reference = variableReference;
        _flag = flag;
    }
    ~ReferenceComparator() {
    }
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const  override {
        if (_flag) {
            return *_reference->getPointer(b) < *_reference->getPointer(a);
        } else {
            return *_reference->getPointer(a) < *_reference->getPointer(b);
        }
    }
    void setReverseFlag(bool flag) { _flag = flag; }
    bool getReverseFlag() const { return _flag; }
    std::string getType() const  override { return "reference"; }
protected:
    const matchdoc::Reference<T> *_reference;
    bool _flag;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(rank, ReferenceComparator, T);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_REFERENCECOMPARATOR_H
