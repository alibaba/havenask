#ifndef ISEARCH_INDEX_TYPE_WRAPPER_H_
#define ISEARCH_INDEX_TYPE_WRAPPER_H_

#include <ha3/index/index.h>
#include <indexlib/index/normal/inverted_index/accessor/term_match_data.h>
#include <indexlib/index/term_meta.h>

BEGIN_HA3_NAMESPACE(rank);

typedef index::TermMatchData TermMatchData;
typedef index::TermMatchDataPtr TermMatchDataPtr;

typedef index::TermMeta TermMeta;
typedef index::TermMetaPtr TermMetaPtr;

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_TERMMATCHDATA_H_
