#include "build_service/config/GenerationMeta.h"
#include "build_service/util/FileUtil.h"
#include <fslib/fslib.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil::legacy;

using namespace build_service::util;

namespace build_service {
namespace config {

BS_LOG_SETUP(config, GenerationMeta);

const string GenerationMeta::FILE_NAME = "generation_meta";

GenerationMeta::GenerationMeta() {
}

GenerationMeta::~GenerationMeta() {
}

bool GenerationMeta::loadFromFile(const string &indexPartitionDir) {
    string fileName = FileUtil::joinFilePath(indexPartitionDir, FILE_NAME);
    if (fslib::EC_FALSE == fslib::fs::FileSystem::isExist(fileName)) {
        string generationDir = FileUtil::getParentDir(indexPartitionDir);
        fileName = FileUtil::joinFilePath(generationDir, FILE_NAME);
        if (fslib::EC_FALSE == fslib::fs::FileSystem::isExist(fileName)) {
            return true;
        }
    }
    string context;
    if (!FileUtil::readFile(fileName, context)) {
        string errorMsg = "load generation meta[" + fileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    try {
        FromJsonString(_meta, context);
    } catch (const autil::legacy::ExceptionBase &e) {
        string errorMsg = "parse generation meta[" + context + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationMeta::operator==(const GenerationMeta &other) const {
    return _meta == other._meta;
}

}
}
