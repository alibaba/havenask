#include <ha3/config/GenerationMeta.h>
#include <suez/turing/common/FileUtil.h>
#include <fslib/fslib.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil::legacy;
using namespace suez::turing;


BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, GenerationMeta);

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
    if (!FileUtil::readFromFslibFile(fileName, context)) {
        return false;
    }
    try {
        FromJsonString(_meta, context);
    } catch (const autil::legacy::ExceptionBase &e) {
        HA3_LOG(ERROR, "parse generation meta[%s] failed.", context.c_str());
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(config);

