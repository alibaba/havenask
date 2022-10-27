#include <ha3/common/QueryTerminator.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, QueryTerminator);

QueryTerminator::QueryTerminator()
    : _isTerminated(false)
{
}

QueryTerminator::~QueryTerminator() { 
}

END_HA3_NAMESPACE(common);

