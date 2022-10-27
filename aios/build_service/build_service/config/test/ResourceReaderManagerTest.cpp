#include "build_service/test/unittest.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/ResourceReader.h"
#include <autil/LoopThread.h>
#include <autil/Lock.h>

using namespace std;
using namespace testing;

namespace build_service {
namespace config {

class ResourceReaderManagerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    static void *parallelGetReader(void *p);
};

void ResourceReaderManagerTest::setUp() {
    ResourceReaderManager::GetInstance()->_readerCache.clear();
}

void ResourceReaderManagerTest::tearDown() {
}
#ifdef DEBUG
//don't run these cases in release mode
TEST_F(ResourceReaderManagerTest, testSimple) {
    setenv(BS_ENV_ENABLE_RESOURCE_READER_CACHE.c_str(), "true", true);
    ResourceReaderManager* manager = ResourceReaderManager::GetInstance();
    manager->_cacheExpireTime = 2;
    ResourceReaderPtr reader;
    {
        ResourceReaderPtr reader1 =
            ResourceReaderManager::getResourceReader(_testDataPath + "configPath1");
        ResourceReaderPtr reader2 =
            ResourceReaderManager::getResourceReader(_testDataPath + "configPath2");
        ResourceReaderPtr reader3 =
            ResourceReaderManager::getResourceReader(_testDataPath + "configPath3");
        size_t cacheSize = manager->_readerCache.size();
        // other case will enlarge cache size
        ASSERT_TRUE(cacheSize >= 3);
        ResourceReaderPtr reader4 =
            ResourceReaderManager::getResourceReader(_testDataPath + "configPath3");
        size_t cacheSize2 = manager->_readerCache.size();
        ASSERT_EQ(cacheSize2, cacheSize);
        ASSERT_EQ(reader3, reader4);
        reader = reader2;
    }
    sleep(4);
    ASSERT_TRUE(manager->_readerCache.end() ==
                manager->_readerCache.find(_testDataPath + "configPath1"));
    ASSERT_TRUE(manager->_readerCache.end() ==
                manager->_readerCache.find(_testDataPath + "configPath3"));
    ASSERT_TRUE(manager->_readerCache.end() !=
                manager->_readerCache.find(_testDataPath + "configPath2"));
    
    reader.reset();
    sleep(4);
    ASSERT_TRUE(manager->_readerCache.end() == 
                manager->_readerCache.find(_testDataPath + "configPath2"));
}

TEST_F(ResourceReaderManagerTest, testCacheExpire) {
    ResourceReaderManager* manager = ResourceReaderManager::GetInstance();
    manager->_cacheExpireTime = 10;
    ResourceReader* pointer1;
    ResourceReader* pointer2;
    int64_t ts1 = -1;
    {
        ResourceReaderPtr reader1 =
            ResourceReaderManager::getResourceReader(_testDataPath + "conf1");
        pointer1 = reader1.get();
        ts1 = manager->_readerCache[_testDataPath + "conf1"].second;
        ASSERT_TRUE(reader1);
        ResourceReaderPtr reader2 =
            ResourceReaderManager::getResourceReader(_testDataPath + "conf2");
        pointer2 = reader2.get();
        ASSERT_TRUE(reader2);
        size_t cacheSize = manager->_readerCache.size();
        // other case will enlarge cache size
        ASSERT_TRUE(cacheSize >= 2);
    }
    sleep(4);
    ASSERT_EQ(ResourceReaderManager::getResourceReader(_testDataPath + "conf1").get(),
              pointer1);
    int64_t ts3 = manager->_readerCache[_testDataPath + "conf1"].second;
    ASSERT_TRUE(ts3 > ts1);

    sleep(8);
    ASSERT_EQ(ResourceReaderManager::getResourceReader(_testDataPath + "conf1").get(),
              pointer1);
    
    // cache for reader2 is expire
    ASSERT_EQ(manager->_readerCache.end(), manager->_readerCache.find(_testDataPath + "conf2"));
    ASSERT_TRUE(ResourceReaderManager::getResourceReader(_testDataPath + "conf3"));
    ASSERT_NE(ResourceReaderManager::getResourceReader(_testDataPath + "conf2").get(),
              pointer2);  
}
#endif
void *ResourceReaderManagerTest::parallelGetReader(void *p) {
    for (int i = 0; i < 10000; i++) {
        stringstream configPath;
        configPath << "/conf" << i%10;
        ResourceReaderManager::getResourceReader(configPath.str());
    }
    return NULL;
}

TEST_F(ResourceReaderManagerTest, testParallelGetReader) {
    int threadNum = 10;
    pthread_t runner[threadNum];
    for (int i = 0; i < threadNum; i++) {
        int ret =  pthread_create(&runner[i], NULL,
                                  &ResourceReaderManagerTest::parallelGetReader, this);
        sleep(1);
        if(ret != 0) {
            printf("Error: pthread_create() failed\n");
            assert(false);
            exit(EXIT_FAILURE);
        }
    }
    for (int i = 0; i < threadNum; i++) {
        pthread_join(runner[i], 0);
    }
}

}
}
