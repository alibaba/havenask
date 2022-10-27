#include <ha3/search/SortExpression.h>

using namespace suez::turing;;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SortExpression);

SortExpression::SortExpression(
        AttributeExpression *attributeExpression,
        bool sortFlag) 
    : _sortFlag(sortFlag), 
      _attributeExpression(attributeExpression)
{
}

SortExpression::~SortExpression() { 
}

END_HA3_NAMESPACE(search);

