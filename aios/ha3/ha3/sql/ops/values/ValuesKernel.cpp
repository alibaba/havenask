#include <ha3/sql/ops/values/ValuesKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, ValuesKernel);
ValuesKernel::ValuesKernel()
    : _memoryPoolResource(nullptr)
{
}

ValuesKernel::~ValuesKernel() {
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *ValuesKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("ValuesKernel");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool ValuesKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    json.Jsonize("output_fields", _outputFields);
    json.Jsonize("output_fields_type", _outputTypes);
    if (_outputFields.size() != _outputTypes.size()) {
        SQL_LOG(ERROR, "output fields size[%lu] not equal output types size[%lu]",
                        _outputFields.size(), _outputTypes.size());
        return false;
    }
    KernelUtil::stripName(_outputFields);
    return true;
}

navi::ErrorCode ValuesKernel::init(navi::KernelInitContext& context) {
    _memoryPoolResource = context.getResource<navi::MemoryPoolResource>(
            navi::RESOURCE_MEMORY_POOL_URI);
    KERNEL_REQUIRES(_memoryPoolResource, "get mem pool resource failed");
    return navi::EC_NONE;
}

navi::ErrorCode ValuesKernel::compute(navi::KernelComputeContext &runContext) {
#define CHECK_DECLARE_COLUMN(columnData)                                \
    if (columnData == nullptr) {                                        \
        SQL_LOG(ERROR, "declare column [%s] failed, type [%s]", \
                        _outputFields[i].c_str(), _outputTypes[i].c_str()); \
        return navi::EC_ABORT;                                          \
    }

    TablePtr table(new Table(_memoryPoolResource->getPool()));
    for (size_t i = 0; i < _outputFields.size(); ++i) {
        if (_outputTypes[i] == "BOOLEAN") {
            auto columnData = TableUtil::declareAndGetColumnData<bool>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "TINYINT" ||
                   _outputTypes[i] == "SMALLINT" ||
                   _outputTypes[i] == "INTEGER" ||
                   _outputTypes[i] == "BIGINT")
        {
            auto columnData = TableUtil::declareAndGetColumnData<int64_t>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "DECIMAL" ||
                   _outputTypes[i] == "FLOAT" ||
                   _outputTypes[i] == "REAL" ||
                   _outputTypes[i] == "DOUBLE")
        {
            auto columnData = TableUtil::declareAndGetColumnData<double>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else if (_outputTypes[i] == "VARCHAR") {
            auto columnData = TableUtil::declareAndGetColumnData<autil::MultiChar>(table, _outputFields[i], false);
            CHECK_DECLARE_COLUMN(columnData);
        } else {
            SQL_LOG(ERROR, "output field [%s] with type [%s] not supported",
                            _outputFields[i].c_str(), _outputTypes[i].c_str());
            return navi::EC_ABORT;
        }
    }
    table->endGroup();
    navi::PortIndex index(0, navi::INVALID_INDEX);
    runContext.setOutput(index, table, true);
    return navi::EC_NONE;
}

REGISTER_KERNEL(ValuesKernel);

END_HA3_NAMESPACE(sql);
