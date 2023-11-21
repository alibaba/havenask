#include "build_service_tasks/extract_doc/test/FakeSwiftOutput.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/io/SwiftOutput.h"
#include "build_service/util/Log.h"
#include "swift/client/MessageInfo.h"
#include "swift/common/MessageInfo.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::document;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, FakeSwiftOutput);

FakeSwiftOutput::FakeSwiftOutput(const config::TaskOutputConfig& outputConfig) : SwiftOutput(outputConfig) {}

FakeSwiftOutput::~FakeSwiftOutput() {}

bool FakeSwiftOutput::write(autil::legacy::Any& any)
{
    try {
        auto msgInfo = AnyCast<swift::client::MessageInfo>(any);
        return write(msgInfo);
    } catch (const BadAnyCast& e) {
        BS_LOG(ERROR, "BadAnyCast: %s", e.what());
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
