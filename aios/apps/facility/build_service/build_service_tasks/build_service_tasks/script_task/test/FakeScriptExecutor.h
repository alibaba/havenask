#pragma once
#include <iosfwd>
#include <string>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/script_task/ScriptExecutor.h"

using namespace std;

namespace build_service { namespace task_base {

class FakeScriptExecutor : public ScriptExecutor
{
private:
    virtual string getBinaryPath() override { return _binaryPath; }

public:
    void setBinaryPath(string binaryPath) { _binaryPath = binaryPath; }

private:
    string _binaryPath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ScriptExecutor);

}} // namespace build_service::task_base
