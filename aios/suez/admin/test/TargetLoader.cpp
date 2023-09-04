#include "TargetLoader.h"

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/fast_jsonizable.h"
#include "fslib/fs/FileSystem.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TargetLoader);

bool TargetLoader::loadFromFile(const std::string &fileName, JsonNodeRef::JsonMap &jsonMap) {
    std::string content;
    auto ec = fslib::fs::FileSystem::readFile(fileName, content);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "read %s failed", fileName.c_str());
        return false;
    }

    Any json;
    try {
        FastFromJsonString(json, content);
        jsonMap = AnyCast<JsonMap>(json);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse %s failed, error: %s", content.c_str(), e.what());
        return false;
    }
    return true;
}

} // namespace suez
