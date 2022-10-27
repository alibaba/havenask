#include "build_service/test/unittest.h"
#include "build_service/reader/ReaderManager.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/PlugInManager.h"

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

namespace build_service {
namespace reader {

class ReaderManagerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    ModuleInfo _moduleInfo;
};

void ReaderManagerTest::setUp() {
    _moduleInfo.moduleName = "CustomRawDocumentReader";
    _moduleInfo.modulePath = "libcustom_reader.so";
}

void ReaderManagerTest::tearDown() {
}

TEST_F(ReaderManagerTest, testGetPlugInManager) {
    ResourceReaderPtr resourceReader(new ResourceReader(
                    TEST_DATA_PATH"/custom_rawdoc_reader_test/"));
    ReaderManager manager(resourceReader);
    PlugInManagerPtr managerPtr1 = manager.getPlugInManager(_moduleInfo);
    ASSERT_TRUE(managerPtr1);
    PlugInManagerPtr managerPtr2 = manager.getPlugInManager(_moduleInfo);
    ASSERT_EQ(managerPtr1, managerPtr2);
}    

TEST_F(ReaderManagerTest, testGetModuleFactory) {
    ResourceReaderPtr resourceReader(new ResourceReader(
                    TEST_DATA_PATH"/custom_rawdoc_reader_test/"));
    ReaderManager manager1(resourceReader);
    ReaderModuleFactory *factory1 = manager1.getModuleFactory(_moduleInfo);
    EXPECT_TRUE(factory1);

    ReaderManager manager2(resourceReader);
    ModuleInfo moduleInfo2(_moduleInfo);
    moduleInfo2.moduleName = "heheh";
    ReaderModuleFactory *factory2 = manager2.getModuleFactory(moduleInfo2);
    EXPECT_TRUE(factory2);

    ReaderManager manager3(resourceReader);
    ModuleInfo moduleInfo3(_moduleInfo);
    moduleInfo3.modulePath = "heheh";
    ReaderModuleFactory *factory3 = manager3.getModuleFactory(moduleInfo3);
    EXPECT_FALSE(factory3);
}

TEST_F(ReaderManagerTest, testGet) {
    ResourceReaderPtr resourceReader(new ResourceReader(
                    TEST_DATA_PATH"/custom_rawdoc_reader_test/"));
    ReaderManager manager1(resourceReader);
    RawDocumentReader *reader1 = manager1.getRawDocumentReaderByModule(_moduleInfo);
    EXPECT_TRUE(reader1);
    delete reader1;

    ReaderManager manager2(resourceReader);
    ModuleInfo moduleInfo2(_moduleInfo);
    moduleInfo2.moduleName = "heheh";
    RawDocumentReader *reader2 = manager2.getRawDocumentReaderByModule(moduleInfo2);
    EXPECT_FALSE(reader2);

    ReaderManager manager3(resourceReader);
    ModuleInfo moduleInfo3(_moduleInfo);
    moduleInfo3.modulePath = "heheh";
    RawDocumentReader *reader3 = manager3.getRawDocumentReaderByModule(moduleInfo3);
    EXPECT_FALSE(reader3);
}

}
}
