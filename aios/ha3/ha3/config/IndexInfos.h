#ifndef ISEARCH_INDEXINFOS_H
#define ISEARCH_INDEXINFOS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <map>
#include <vector>
#include <string>
#include <ha3/config/FieldBoostTable.h>
#include <suez/turing/expression/util/IndexInfos.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::IndexInfo IndexInfo; 
typedef suez::turing::IndexInfos IndexInfos; 

END_HA3_NAMESPACE(config);

#endif //ISEARCH_INDEXINFOS_H
