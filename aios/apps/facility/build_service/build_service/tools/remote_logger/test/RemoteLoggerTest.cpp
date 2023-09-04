#include "build_service/tools/remote_logger/RemoteLogger.h"

#include "alog/Appender.h"
#include "autil/NetUtil.h"
#include "autil/Thread.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "fslib/fs/local/LocalFile.h"
#include "fslib/fs/local/LocalFileSystem.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

using fslib::ErrorCode;
using fslib::fs::FileSystem;

namespace build_service::tools {
class RemoteLoggerTest : public TESTBASE
{
public:
    RemoteLoggerTest() = default;
    ~RemoteLoggerTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void RemoteLoggerTest::setUp() {}

void RemoteLoggerTest::tearDown() {}

TEST_F(RemoteLoggerTest, testUsage)
{
    std::string logFile(GET_TEMP_DATA_PATH());

    logFile += "/pipe_access.log";
    alog::Appender* appender = new alog::PipeAppender(logFile.c_str());
    auto logger = alog::Logger::getLogger("test_logger");
    logger->setAppender(appender);

    std::string srcDir = GET_TEMP_DATA_PATH() + "/src";
    std::string targetDir = GET_TEMP_DATA_PATH() + "/target";

    std::string srcDataPath = GET_PRIVATE_TEST_DATA_PATH() + "/log_uploader";
    std::string tmpDataPath = GET_TEMP_DATA_PATH() + "/src/config/log_uploader";
    std::string jsonStr;
    fslib::util::FileUtil::readFile(srcDataPath + "/build_app.json.template", jsonStr);
    autil::StringUtil::replaceAll(jsonStr, "$template", targetDir.c_str());
    fslib::util::FileUtil::atomicCopy(srcDataPath, tmpDataPath);

    RemoteLogger uploader;
    uploader._TEST_cwd = "/slave/disk_links/1/"
                         "bs_biz_order_his_na630_hdd_compare_xiaohao.biz_order_summary.999.builder.full.32768.49151."
                         "biz_order_summary_34_72/log_uploader";
    std::atomic<bool> logging(true);
    int32_t logCount = 0;
    auto runThread = autil::Thread::createThread([&]() { ASSERT_TRUE(uploader.run(srcDir, logFile, 10)); });
    auto logThread = autil::Thread::createThread([&]() {
        while (logging) {
            usleep(500 * 1000);
            ALOG_LOG(logger, alog::LOG_LEVEL_WARN, "#");
            logCount++;
        }
        std::cerr << "log done" << std::endl;
    });
    fslib::util::FileUtil::writeFile(tmpDataPath + "/build_app.json", jsonStr);
    std::cerr << "config ready" << std::endl;
    while (logCount < 3) {
        usleep(100);
    }
    logging = false;
    logThread->join();
    logger->removeAllAppenders();
    delete appender;
    uploader.stop();
    runThread->join();
    targetDir += "/biz_order_summary/generation_999/logs/builder/32768_49151/";
    std::string ip;
    ASSERT_TRUE(autil::NetUtil::GetDefaultIp(ip));
    std::string content;
    std::string preFix = ip + "_";
    preFix += std::to_string(uploader._startTime);
    std::string logFileName = preFix + "_bs.log_0";
    std::string fullTargetPath = FileSystem::joinFilePath(targetDir, logFileName);
    ASSERT_EQ(fslib::EC_TRUE, FileSystem::isExist(fullTargetPath)) << fullTargetPath;
    ASSERT_EQ(fslib::EC_OK, FileSystem::readFile(fullTargetPath, content));

    size_t realLogCount = 0;
    for (size_t i = 0; i < content.size(); ++i) {
        if (content[i] == '#') {
            realLogCount++;
        }
    }
    ASSERT_EQ(logCount, realLogCount);
}

TEST_F(RemoteLoggerTest, testCleanExpiredLogs)
{
    std::string dir = GET_TEMP_DATA_PATH();
    std::string fileName = GET_TEMP_DATA_PATH() + "/dummy";
    fslib::util::FileUtil::writeFile(fileName, "dummy");
    sleep(3);
    bool isExist = false;
    ASSERT_TRUE(fslib::util::FileUtil::isExist(fileName, isExist));
    ASSERT_TRUE(isExist);

    RemoteLogger::cleanExpiredLogs(dir, 1);
    ASSERT_TRUE(fslib::util::FileUtil::isExist(fileName, isExist));
    ASSERT_TRUE(isExist);

    RemoteLogger::cleanExpiredLogs(dir, 0);
    isExist = false;
    ASSERT_TRUE(fslib::util::FileUtil::isExist(fileName, isExist));
    ASSERT_FALSE(isExist);
}

class SlowWriteFileSystem : public fslib::fs::LocalFileSystem
{
public:
    fslib::fs::File* openFile(const std::string& rawFileName, fslib::Flag mode) override
    {
        std::string prefix = "slow://";
        std::string realPath = rawFileName.substr(prefix.length());
        fslib::fs::LocalFile* localFile =
            dynamic_cast<fslib::fs::LocalFile*>(fslib::fs::LocalFileSystem::openFile(realPath, mode));
        return new SlowFile(localFile);
    }

public:
    class SlowFile : public fslib::fs::File
    {
    public:
        SlowFile(fslib::fs::LocalFile* localFile) : fslib::fs::File(localFile->getFileName()), _localFile(localFile)
        {
            assert(_localFile);
        }
        ~SlowFile()
        {
            std::cerr << "total write count " << _writeCount << std::endl;
            if (_localFile) {
                DELETE_AND_SET_NULL(_localFile);
            }
        }
        ssize_t read(void* buffer, size_t length) override { return _localFile->read(buffer, length); }
        ssize_t pread(void* buffer, size_t length, off_t offset) override
        {
            return _localFile->pread(buffer, length, offset);
        }
        ErrorCode flush() override { return _localFile->flush(); }
        ErrorCode close() override { return _localFile->close(); }
        ErrorCode seek(int64_t offset, fslib::SeekFlag flag) override { return _localFile->seek(offset, flag); }
        int64_t tell() override { return _localFile->tell(); }
        bool isOpened() const override { return _localFile->isOpened(); }
        bool isEof() override { return _localFile->isEof(); }
        ssize_t pwrite(const void* buffer, size_t length, off_t offset) override
        {
            usleep(100 * 1000); // 100ms
            _writeCount++;
            return _localFile->pwrite(buffer, length, offset);
        }
        ssize_t write(const void* buffer, size_t length) override
        {
            usleep(1000 * 1000); // 1s
            _writeCount++;
            return _localFile->write(buffer, length);
        }

    private:
        fslib::fs::LocalFile* _localFile = nullptr;
        size_t _writeCount = 0;
    };
};

TEST_F(RemoteLoggerTest, testSlowWrite)
{
    auto fsFactory = fslib::fs::FileSystemFactory::getInstance();
    SlowWriteFileSystem* slowFileSystem = new SlowWriteFileSystem();
    fsFactory->setFileSystem("slow", slowFileSystem);
    std::string logFile(GET_TEMP_DATA_PATH());

    logFile += "/pipe_access.log";
    alog::Appender* appender = new alog::PipeAppender(logFile.c_str());
    auto logger = alog::Logger::getLogger("test_logger");
    logger->setAppender(appender);
    logger->setInheritFlag(false);

    std::string srcDir = GET_TEMP_DATA_PATH() + "/src";
    std::string targetDir = GET_TEMP_DATA_PATH() + "/target";

    std::string srcDataPath = GET_PRIVATE_TEST_DATA_PATH() + "/log_uploader";
    std::string tmpDataPath = GET_TEMP_DATA_PATH() + "/src/config/log_uploader";
    std::string jsonStr;
    fslib::util::FileUtil::readFile(srcDataPath + "/build_app.json.template", jsonStr);
    std::string slowTargetDir = "slow://" + targetDir;
    autil::StringUtil::replaceAll(jsonStr, "$template", slowTargetDir.c_str());
    fslib::util::FileUtil::atomicCopy(srcDataPath, tmpDataPath);

    RemoteLogger uploader;
    uploader._TEST_cwd = "/slave/disk_links/1/"
                         "bs_biz_order_his_na630_hdd_compare_xiaohao.biz_order_summary.999.builder.full.32768.49151."
                         "biz_order_summary_34_72/log_uploader";
    int32_t logCount = 0;
    auto runThread = autil::Thread::createThread([&]() { ASSERT_TRUE(uploader.run(srcDir, logFile, 10)); });
    sleep(1);
    auto logThread = autil::Thread::createThread([&]() {
        for (size_t i = 0; i < 1000; ++i) {
            ALOG_LOG(logger, alog::LOG_LEVEL_WARN, "#");
            logCount++;
        }
        std::cerr << "log done" << std::endl;
    });
    fslib::util::FileUtil::writeFile(tmpDataPath + "/build_app.json", jsonStr);
    std::cerr << "config ready" << std::endl;

    logThread->join();
    logger->flushAll();
    logger->removeAllAppenders();
    delete appender;
    uploader.stop();
    runThread->join();
    targetDir += "/biz_order_summary/generation_999/logs/builder/32768_49151/";
    std::string ip;
    ASSERT_TRUE(autil::NetUtil::GetDefaultIp(ip));
    std::string content;
    std::string preFix = ip + "_";
    preFix += std::to_string(uploader._startTime);
    std::string logFileName = preFix + "_bs.log_0";
    std::string fullTargetPath = FileSystem::joinFilePath(targetDir, logFileName);
    ASSERT_EQ(fslib::EC_TRUE, FileSystem::isExist(fullTargetPath)) << fullTargetPath;
    ASSERT_EQ(fslib::EC_OK, FileSystem::readFile(fullTargetPath, content));
}
} // namespace build_service::tools
