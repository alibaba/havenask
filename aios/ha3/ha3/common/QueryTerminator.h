#ifndef ISEARCH_QUERYTERMINATOR_H
#define ISEARCH_QUERYTERMINATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/QueryTerminator.h>
BEGIN_HA3_NAMESPACE(common);

typedef suez::turing::QueryTerminator QueryTerminator; 
typedef suez::turing::QueryTerminatorPtr QueryTerminatorPtr; 

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYTERMINATOR_H
