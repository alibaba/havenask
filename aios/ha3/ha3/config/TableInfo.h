#ifndef ISEARCH_TABLEINFO_H
#define ISEARCH_TABLEINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/FieldInfos.h>
#include <ha3/config/IndexInfos.h>
#include <ha3/config/AttributeInfos.h>
#include <ha3/config/QueryInfo.h>
#include <ha3/config/ResultConfigInfo.h>
#include <ha3/config/SummaryInfo.h>
#include <suez/turing/expression/util/TableInfo.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::TableInfo TableInfo; 
typedef std::map<std::string, suez::turing::TableInfoPtr> ClusterTableInfoMap;
HA3_TYPEDEF_PTR(ClusterTableInfoMap);

typedef suez::turing::TableMap TableMap; 
HA3_TYPEDEF_PTR(TableMap);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_TABLEINFO_H
