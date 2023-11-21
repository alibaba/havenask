#include <string>
#include <vector>

#include "iquan/common/Status.h"
#include "iquan/common/catalog/CatalogDef.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/SqlSuiteInfo.h"

namespace iquan {

struct CatalogData {
    CatalogDefs catalogDefs;
    DefaultCatalogPath defaultCatalogPath;
};

struct RunOptions {
    std::string signature;
    int numThread;
    int numCycles;
    std::string queryFile;
    bool debug;
    autil::legacy::json::JsonMap sqlParams;
};

class IquanPerfTestBase : public IquanTestBase {
public:
    static Status loadCatalogData(const std::string &rootPath, CatalogData &catalogData);

    static Status registerCatalogData(const CatalogDefs &catalogDefs, IquanPtr &pIquan);

    static Status runTest(const DefaultCatalogPath &defaultCatalogPath,
                          IquanPtr &pIquan,
                          const RunOptions &runOptions);

private:
    static Status loadSqlQueryInfos(const std::string &filePath, SqlQueryInfos &sqlQueryInfos);

    static Status runSingleThread(const DefaultCatalogPath &defaultCatalogPath,
                                  IquanPtr &pIquan,
                                  const RunOptions &runOptions,
                                  const SqlQueryInfos &sqlQueryInfos);
};

} // namespace iquan
