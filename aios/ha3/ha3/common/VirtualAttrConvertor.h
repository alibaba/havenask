#ifndef ISEARCH_VIRTUALATTRCONVERTOR_H
#define ISEARCH_VIRTUALATTRCONVERTOR_H
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <suez/turing/expression/util/VirtualAttrConvertor.h>

BEGIN_HA3_NAMESPACE(common);

typedef suez::turing::VirtualAttrConvertor VirtualAttrConvertor; 
HA3_TYPEDEF_PTR(VirtualAttrConvertor);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_VIRTUALATTRCONVERTOR_H
