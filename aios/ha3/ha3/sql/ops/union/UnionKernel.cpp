#include <ha3/sql/ops/union/UnionKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, UnionKernel);
UnionKernel::UnionKernel()
{
}

UnionKernel::~UnionKernel() {
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *UnionKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("UnionKernel");
    KERNEL_REGISTER_ADD_INPUT_GROUP_OPTIONAL(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

navi::ErrorCode UnionKernel::compute(navi::KernelComputeContext &runContext) {
    navi::IndexType inputGroupIndex = 0;
    navi::GroupDatas datas;
    if (!runContext.getInputGroupDatas(inputGroupIndex, datas)) {
        SQL_LOG(ERROR, "get input group failed");
        return navi::EC_ABORT;
    }
    for (auto data : datas) {
        if (data != nullptr) {
            if (!doCompute(data)) {
                return navi::EC_ABORT;
            }
        }
    }
    bool eof = false;
    runContext.inputGroupEof(inputGroupIndex, eof);
    if (eof) {
        outputResult(runContext);
    }
    return navi::EC_NONE;
}

void UnionKernel::outputResult(navi::KernelComputeContext &runContext) {
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    runContext.setOutput(outputIndex, _table, true);
}

bool UnionKernel::doCompute(const navi::DataPtr &data) {
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

REGISTER_KERNEL(UnionKernel);

END_HA3_NAMESPACE(sql);
