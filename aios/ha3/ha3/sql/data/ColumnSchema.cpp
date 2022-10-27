#include <ha3/sql/data/ColumnSchema.h>
#include <ha3/sql/data/TableUtil.h>


using namespace std;

BEGIN_HA3_NAMESPACE(sql);

ColumnSchema::ColumnSchema(std::string name, ValueType type)
    : _name(name)
    , _type(type)
{
}

ColumnSchema::~ColumnSchema() {
}

std::string ColumnSchema::toString() const {
    return "name:" + _name + ",type:" + TableUtil::valueTypeToString(_type);
}

END_HA3_NAMESPACE(sql);
