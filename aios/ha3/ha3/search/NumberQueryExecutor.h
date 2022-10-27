#ifndef ISEARCH_NUMBERQUERYEXECUTOR_H
#define ISEARCH_NUMBERQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class NumberQueryExecutor : public TermQueryExecutor
{
public:
    NumberQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter,
                        const common::Term &term);
    ~NumberQueryExecutor();
public:
    const std::string getName() const { return "NumberQueryExecutor";}
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_NUMBERQUERYEXECUTOR_H
