#include <ha3/turing/variant/SqlTableVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>

using namespace std;
using namespace tensorflow;
using namespace autil;

USE_HA3_NAMESPACE(sql);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, SqlTableVariant);

SqlTableVariant::SqlTableVariant()
    : _pool(NULL)
    , _flag(0)
{}

SqlTableVariant::SqlTableVariant(const std::shared_ptr<autil::mem_pool::Pool> &poolPtr)
    : _table(new Table(poolPtr))
    , _pool(poolPtr.get())
    , _flag(0)
{
}

SqlTableVariant::~SqlTableVariant() {
    _table.reset();
    _pool = NULL;
}

SqlTableVariant::SqlTableVariant(const SqlTableVariant &other)
    : _table(other._table)
    , _pool(other._pool)
    , _metadata(other._metadata)
    , _flag(other._flag)
{
}

void SqlTableVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    if (_table != nullptr) {
        data->metadata_.append(1, char(0));
        _table->serializeToString(data->metadata_, _pool);
    } else {
        data->metadata_.append(1, char(1));
    }
}

bool SqlTableVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool SqlTableVariant::construct(const std::shared_ptr<autil::mem_pool::Pool> &tablePool,
                                autil::mem_pool::Pool *dataPool)
{
    if (tablePool == nullptr || dataPool == nullptr) {
        return false;
    }
    _table.reset(new Table(tablePool));
    if (_metadata.size() == 0) {
        return false;
    }
    _flag = _metadata[0];
    if (_flag == char(0)) {
        _table->deserializeFromString(_metadata.c_str() + 1, _metadata.size() - 1,  dataPool);
    }
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(SqlTableVariant, "SqlTable");

END_HA3_NAMESPACE(turing);
