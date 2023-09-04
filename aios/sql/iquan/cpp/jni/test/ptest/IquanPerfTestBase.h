#include <string>
#include <vector>

#include "iquan/common/Status.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/SqlSuiteInfo.h"

namespace iquan {

struct CatalogData {
    std::vector<std::string> ha3TableNameList;
    std::vector<std::string> functionNameList;
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

    static Status registerCatalogData(const std::string &rootPath,
                                      const CatalogData &catalogData,
                                      IquanPtr &pIquan);

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
