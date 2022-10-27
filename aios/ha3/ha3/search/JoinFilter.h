#ifndef ISEARCH_JOINFILTER_H
#define ISEARCH_JOINFILTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>

BEGIN_HA3_NAMESPACE(search);

class JoinFilter
{
public:
    JoinFilter(suez::turing::JoinDocIdConverterCreator *convertFactory, bool forceStrongJoin);
    ~JoinFilter();
private:
    JoinFilter(const JoinFilter &);
    JoinFilter& operator=(const JoinFilter &);
public:
    bool pass(matchdoc::MatchDoc doc) {
        return doPass(doc, false);
    }
    bool passSubDoc(matchdoc::MatchDoc doc) {
        return doPass(doc, true);
    }
    uint32_t getFilteredCount() const {
        return _filteredCount;
    }
private:
    bool doPass(matchdoc::MatchDoc doc, bool isSub);
private:
    suez::turing::JoinDocIdConverterCreator *_convertFactory;
    uint32_t _filteredCount;
    bool _forceStrongJoin;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JoinFilter);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_JOINFILTER_H
