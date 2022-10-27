#include "build_service/tools/partition_split_merger/PartitionSplitChecker.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/FileUtil.h"
#include <indexlib/index_base/schema_adapter.h>

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

using namespace build_service::config;
using namespace build_service::util;

namespace build_service {
namespace tools {

BS_LOG_SETUP(tools, PartitionSplitChecker);

PartitionSplitChecker::PartitionSplitChecker(){
}

PartitionSplitChecker::~PartitionSplitChecker() {
}

bool PartitionSplitChecker::init(const Param &param) {
    if (!initIndexPartitionSchema(param)) {
        return false;
    }
    return true;
}

bool PartitionSplitChecker::initIndexPartitionSchema(const Param &param) {
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(param.configPath);
    string relativePath = ResourceReader::getClusterConfRelativePath(param.clusterName);
    string tableName;
    if (!resourceReader->getConfigWithJsonPath(relativePath, "cluster_config.table_name", tableName))
    {
        string errorMsg = "get cluster["
                          + param.clusterName + "] table name failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    string schemaFile = ResourceReader::getSchemaConfRelativePath(tableName);
    try {
        _indexPartitionSchemaPtr = SchemaAdapter::LoadSchema(
                param.configPath, schemaFile);
    } catch (const autil::legacy::ExceptionBase &e)  {
        string errorMsg = "load " + schemaFile
                          + " schema failed, exception["
                          + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (!_indexPartitionSchemaPtr) {
        string errorMsg = "load schema " + schemaFile + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool PartitionSplitChecker::check() const {
    IndexSchemaPtr indexSchema = _indexPartitionSchemaPtr->GetIndexSchema();

    size_t indexCount = indexSchema->GetIndexCount();
    for (size_t i = 0; i < indexCount; ++i) {
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig((indexid_t)i);
        if (!indexConfig) {
            return false;
        }
        if (indexConfig->HasTruncate()) {
            string errorMsg = "partition split do not support truncate index";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
        if (indexConfig->HasAdaptiveDictionary()) {
            string errorMsg = "partition split do not support adaptive "
                              "bitmap index ["
                              + indexConfig->GetIndexName() + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

}
}
