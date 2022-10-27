#include "build_service/config/ScriptTaskConfig.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, ScriptTaskConfig);

string ScriptTaskConfig::PYTHON_SCRIPT_TYPE = "python";
string ScriptTaskConfig::SHELL_SCRIPT_TYPE = "shell";

string ScriptTaskConfig::BINARY_RESOURCE_TYPE = "binary";
string ScriptTaskConfig::LIBRARY_RESOURCE_TYPE = "library";

ScriptTaskConfig::ScriptTaskConfig()
    : maxRetryTimes(0)
    , minRetryInterval(0)
    , forceRetry(false)
{}

ScriptTaskConfig::~ScriptTaskConfig() {}

void ScriptTaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("script_type", scriptType);
    json.Jsonize("script_file", scriptFile);
    json.Jsonize("resources", resources, resources);
    json.Jsonize("arguments", arguments, arguments);
    json.Jsonize("max_retry_times", maxRetryTimes, maxRetryTimes);
    json.Jsonize("min_retry_interval", minRetryInterval, minRetryInterval);
    json.Jsonize("force_retry", forceRetry, forceRetry);

}

}
}
