#include <ha3/sql/ops/merge/MergeKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, MergeKernel);
MergeKernel::MergeKernel()
{}

MergeKernel::~MergeKernel() {
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *MergeKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("MergeKernel");
    KERNEL_REGISTER_ADD_INPUT_GROUP_OPTIONAL(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

navi::ErrorCode MergeKernel::compute(navi::KernelComputeContext &runContext) {
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

void MergeKernel::outputResult(navi::KernelComputeContext &runContext) {
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    runContext.setOutput(outputIndex, _table, true);
}

bool MergeKernel::doCompute(const navi::DataPtr &data) {
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

REGISTER_KERNEL(MergeKernel);

END_HA3_NAMESPACE(sql);
