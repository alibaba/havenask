#ifndef ISEARCH_SUMMARYINFO_H
#define ISEARCH_SUMMARYINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <map>
#include <string>
#include <autil/legacy/jsonizable.h>
#include <suez/turing/expression/util/SummaryInfo.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::SummaryInfo SummaryInfo; 

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SUMMARYINFO_H
