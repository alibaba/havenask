#ifndef ISEARCH_ROW_H
#define ISEARCH_ROW_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>

BEGIN_HA3_NAMESPACE(sql);

typedef matchdoc::MatchDoc Row;

HA3_TYPEDEF_PTR(Row);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_ROW_H
