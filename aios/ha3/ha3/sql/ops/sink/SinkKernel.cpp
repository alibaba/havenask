#include <ha3/sql/ops/sink/SinkKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SinkKernel);
SinkKernel::SinkKernel()
{
}

SinkKernel::~SinkKernel() {
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *SinkKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("SinkKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

navi::ErrorCode SinkKernel::init(navi::KernelInitContext& context) {
    return navi::EC_NONE;
}

navi::ErrorCode SinkKernel::compute(navi::KernelComputeContext &runContext) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    runContext.getInput(inputIndex, data, eof);
    if (data != nullptr) {
        if (!doCompute(data)) {
            return navi::EC_ABORT;
        }
    }
    if (eof) {
        outputResult(runContext);
    }
    return navi::EC_NONE;
}

void SinkKernel::outputResult(navi::KernelComputeContext &runContext) {
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    runContext.setOutput(outputIndex, _table, true);
}

bool SinkKernel::doCompute(const navi::DataPtr &data) {
    TablePtr input = dynamic_pointer_cast<Table>(data);
    if (input == nullptr) {
        SQL_LOG(ERROR, "invalid input table");
        return false;
    }
    if (_table == nullptr) {
        _table = input;
    } else {
        if (!_table->merge(input)) {
            SQL_LOG(ERROR, "merge input table failed");
            return false;
        }
    }
    return true;
}

REGISTER_KERNEL(SinkKernel);

END_HA3_NAMESPACE(sql);
