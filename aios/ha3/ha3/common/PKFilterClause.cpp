#include <ha3/common/PKFilterClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, PKFilterClause);

PKFilterClause::PKFilterClause() { 
}

PKFilterClause::~PKFilterClause() { 
}

void PKFilterClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
}

void PKFilterClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
}

std::string PKFilterClause::toString() const {
    return _originalString;
}

END_HA3_NAMESPACE(common);

