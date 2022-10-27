#include <ha3/search/BitmapTermQueryExecutor.h>

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, BitmapTermQueryExecutor);

BitmapTermQueryExecutor::BitmapTermQueryExecutor(PostingIterator *iter, 
        const Term &term)
    : TermQueryExecutor(iter, term)
{
    initBitmapIterator();
}

BitmapTermQueryExecutor::~BitmapTermQueryExecutor() { 
}

void BitmapTermQueryExecutor::reset() {
    TermQueryExecutor::reset();
    initBitmapIterator();
}

END_HA3_NAMESPACE(search);

