#include <ha3/sql/data/Column.h>
#include <ha3/sql/data/DataCommon.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);

Column::Column(ColumnSchema* columnSchema, ColumnDataBase* columnData)
    : _schema(columnSchema)
    , _data(columnData)
{
}

Column::~Column() {
    _schema = NULL;
    DELETE_AND_SET_NULL(_data);
}

END_HA3_NAMESPACE(sql);

