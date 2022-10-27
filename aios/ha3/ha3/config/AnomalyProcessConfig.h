#ifndef ISEARCH_ANOMALYPROCESSCONFIG_H
#define ISEARCH_ANOMALYPROCESSCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

typedef autil::legacy::json::JsonMap AnomalyProcessConfig;
typedef AnomalyProcessConfig AnomalyProcessConfigT;
typedef AnomalyProcessConfig SqlAnomalyProcessConfig;
HA3_TYPEDEF_PTR(AnomalyProcessConfig);
typedef autil::legacy::json::JsonMap AnomalyProcessConfigMap;
HA3_TYPEDEF_PTR(AnomalyProcessConfigMap);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_ANOMALYPROCESSCONFIG_H
