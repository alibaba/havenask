#ifndef ISEARCH_SORTERCONFIGINFO_H
#define ISEARCH_SORTERCONFIGINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <suez/turing/expression/plugin/SorterConfig.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::SorterInfo SorterConfigInfo; 
typedef std::vector<SorterConfigInfo> SorterConfigInfos;

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SORTERCONFIGINFO_H
