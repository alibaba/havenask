#include <ha3/search/NumberQueryExecutor.h>

IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, NumberQueryExecutor);

NumberQueryExecutor::NumberQueryExecutor(PostingIterator *iter,
        const common::Term &term)
    : TermQueryExecutor(iter, term) {}

NumberQueryExecutor::~NumberQueryExecutor() {}

END_HA3_NAMESPACE(search);

