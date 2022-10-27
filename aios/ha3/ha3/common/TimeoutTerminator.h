#ifndef ISEARCH_TIMEOUTTERMINATOR_H
#define ISEARCH_TIMEOUTTERMINATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/TimeoutTerminator.h>

BEGIN_HA3_NAMESPACE(common);

typedef suez::turing::TimeoutTerminator TimeoutTerminator;
typedef suez::turing::TimeoutTerminatorPtr TimeoutTerminatorPtr;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TIMEOUTTERMINATOR_H
