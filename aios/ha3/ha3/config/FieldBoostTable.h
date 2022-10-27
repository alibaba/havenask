#ifndef ISEARCH_FIELDBOOSTTABLE_H
#define ISEARCH_FIELDBOOSTTABLE_H

#include <vector>

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/FieldBoost.h>
#include <suez/turing/expression/util/FieldBoostTable.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::FieldBoostTable FieldBoostTable; 

END_HA3_NAMESPACE(config);

#endif //ISEARCH_FIELDBOOSTTABLE_H
