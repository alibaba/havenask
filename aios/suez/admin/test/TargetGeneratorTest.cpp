#include "suez/admin/TargetGenerator.h"

#include "autil/EnvUtil.h"
#include "catalog/service/CatalogControllerManager.h"
#include "catalog/service/CatalogServiceImpl.h"
#include "catalog/store/LocalStore.h"
#include "catalog/store/StoreFactory.h"
#include "catalog/tools/TableSchemaConfig.h"
#include "fslib/fs/FileSystem.h"
#include "google/protobuf/util/json_util.h"
#include "suez/admin/ClusterServiceImpl.h"
#include "suez/sdk/PathDefine.h"
#include "unittest/unittest.h"

using namespace catalog;
using namespace fslib::fs;

namespace suez {

class TargetGeneratorTest : public TESTBASE {
public:
    void setUp() override {
        _testDataPath = GET_TEMPLATE_DATA_PATH();
        StoreFactory::getInstance()->registerStore("LOCAL", []() { return std::make_unique<LocalStore>(); });
        _catalogService = std::make_unique<CatalogServiceImpl>("LOCAL://" + _testDataPath);
        ASSERT_TRUE(_catalogService->start());
        _clusterService = std::make_unique<ClusterServiceImpl>("LOCAL://" + _testDataPath);
        ASSERT_TRUE(_clusterService->start());

        _catalogControllerManager = _catalogService->getManager();
        createCatalog();
        createCluster();
        deployDatabase();
    }

    void createCatalog() {
        catalog::proto::Catalog catalog;
        auto fileName = GET_TEST_DATA_PATH() + "/admin_test/catalog.json";
        std::string content;
        auto ec = fslib::fs::FileSystem::readFile(fileName, content);
        ASSERT_TRUE(ec == fslib::EC_OK);
        autil::StringUtil::replaceAll(content, "$configRoot", _testDataPath);

        catalog::proto::CreateCatalogRequest request;
        catalog::proto::CommonResponse response;
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto status = google::protobuf::util::JsonStringToMessage(content, &request, options);
        ASSERT_TRUE(status.ok()) << status.ToString();

        _catalogService->createCatalog(nullptr, &request, &response, nullptr);
        ASSERT_EQ(catalog::proto::ResponseStatus::OK, response.status().code()) << response.DebugString();
    }

    void createCluster() {
        CreateClusterDeploymentRequest request;
        CommonResponse response;

        auto fileName = GET_TEST_DATA_PATH() + "/admin_test/create_cluster_request.json";
        std::string content;
        auto ec = fslib::fs::FileSystem::readFile(fileName, content);
        ASSERT_TRUE(ec == fslib::EC_OK);
        autil::StringUtil::replaceAll(content, "$configRoot", _testDataPath);

        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto status = google::protobuf::util::JsonStringToMessage(content, &request, options);
        ASSERT_TRUE(status.ok()) << status.ToString();

        _clusterService->createClusterDeployment(nullptr, &request, &response, nullptr);
        ASSERT_EQ(CommonResponse::ERROR_NONE, response.errorcode());
    }

    void deployDatabase() {
        DeployDatabaseRequest request;
        CommonResponse response;
        auto fileName = GET_TEST_DATA_PATH() + "/admin_test/deploy_database_request.json";
        std::string content;
        auto ec = fslib::fs::FileSystem::readFile(fileName, content);
        ASSERT_TRUE(ec == fslib::EC_OK);
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto status = google::protobuf::util::JsonStringToMessage(content, &request, options);
        ASSERT_TRUE(status.ok()) << status.ToString();
        _clusterService->deployDatabase(nullptr, &request, &response, nullptr);
        ASSERT_EQ(CommonResponse::ERROR_NONE, response.errorcode());
    }

    void checkFile(const std::string &f1, const std::string &f2) {
        ASSERT_EQ(fslib::EC_TRUE, FileSystem::isExist(f2));
        std::string c1, c2;
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(f1, c1));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(f2, c2));

        ASSERT_EQ(c1, c2) << "c1:\n " << c1 << "\n, c2:\n" << c2;
    }

protected:
    std::unique_ptr<catalog::CatalogServiceImpl> _catalogService;
    std::unique_ptr<ClusterServiceImpl> _clusterService;
    std::shared_ptr<CatalogControllerManager> _catalogControllerManager;

    std::string _testDataPath;
};

TEST_F(TargetGeneratorTest, testSimple) {
    autil::EnvGuard env1("DIRECT_WRITE_TEMPLATE_CONFIG_PATH", GET_TEST_DATA_PATH() + "/direct_write_templates/");

    TargetGenerator generator(_catalogService->getManager(), _clusterService->getManager());
    auto s = generator.genTarget();
    ASSERT_TRUE(s.is_ok());
    auto target = s.get();

    auto fileName = GET_TEST_DATA_PATH() + "/admin_test/admin_target.json";
    std::string content;
    auto ec = fslib::fs::FileSystem::readFile(fileName, content);
    ASSERT_TRUE(ec == fslib::EC_OK);
    auto configPath =
        _testDataPath +
        "catalog/database/table/1234567/515d73b5f9d8d527fccad77970eebd6c/config.d41d8cd98f00b204e9800998ecf8427e";
    autil::StringUtil::replaceAll(content, "$configPath", configPath);
    autil::StringUtil::replaceAll(content, "$indexRoot", _testDataPath + "table/index");
    AdminTarget expect;
    autil::legacy::FastFromJsonString(expect, content);
    ASSERT_EQ(expect, target) << autil::legacy::FastToJsonString(expect) << ", "
                              << autil::legacy::FastToJsonString(target);

    checkFile(GET_TEST_DATA_PATH() + "/admin_test/table_schema.json", PathDefine::getSchemaPath(configPath, "table"));
    checkFile(GET_TEST_DATA_PATH() + "/admin_test/table_table.json", PathDefine::getDataTablePath(configPath, "table"));
    checkFile(GET_TEST_DATA_PATH() + "/admin_test/table_cluster.json",
              PathDefine::getTableClusterConfigPath(configPath, "table"));
}

} // namespace suez
