#ifndef ISEARCH_BS_SLOWUPDATEPROCESSOR_H
#define ISEARCH_BS_SLOWUPDATEPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include <autil/legacy/jsonizable.h>
#include <autil/LoopThread.h>
#include <autil/Lock.h>

namespace build_service {
namespace processor {

class SlowUpdateProcessor : public DocumentProcessor {
public:
    static const std::string PROCESSOR_NAME;
public:
    enum class ActionType {
        SKIP_ALL,
        SKIP_REALTIME,
        UNKNOWN,
    };

    struct Config : public autil::legacy::Jsonizable {
        Config() = default;
        int64_t timestamp = -1;
        std::string filterKey;
        std::string filterValue;
        ActionType action = ActionType::UNKNOWN;

        Config(const Config &other) = default;
        Config &operator=(const Config &other) = default;
        Config(Config &&other)
            : timestamp(other.timestamp),
              filterKey(std::move(other.filterKey)),
              filterValue(std::move(other.filterValue)),
              action(other.action) {}

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

        void reset() {
            timestamp = -1;
            filterKey.clear();
            filterValue.clear();
            action = ActionType::UNKNOWN;
        }

        bool operator==(const Config &other) const {
            return timestamp == other.timestamp && filterKey == other.filterKey &&
                   filterValue == other.filterValue && action == other.action;
        }
        bool operator!=(const Config &other) const { return !(*this == other); }
    };

    class ExecuteProcessor : public DocumentProcessor {
    public:
        ExecuteProcessor(const SlowUpdateProcessor &baseProcessor)
            : DocumentProcessor(baseProcessor._docOperateFlag),
              _config(baseProcessor.getConfig()) {}
        bool process(const document::ExtendDocumentPtr &document) override;
        void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs) override;
        ExecuteProcessor *clone() override;
        std::string getDocumentProcessorName() const override {
            return SlowUpdateProcessor::PROCESSOR_NAME;
        }

    private:
        SlowUpdateProcessor::Config _config;

    private:
        friend class SlowUpdateProcessor;
    };

private:
    static const std::string REALTIME_START_TIMESTAMP;
    static const std::string FILTER_KEY;
    static const std::string FILTER_VALUE;
    static const std::string ACTION;

    static const std::string ACTION_SKIP_ALL;
    static const std::string ACTION_SKIP_REALTIME;

    static const std::string CONFIG_PATH;
    static const std::string SYNC_INTERVAL;

    static constexpr int64_t DEFAULT_SYNC_INTERVAL = 30; // 30 seconds

public:
    SlowUpdateProcessor(bool autoSync = true);
    ~SlowUpdateProcessor();

    SlowUpdateProcessor(const SlowUpdateProcessor &other) = delete;
    SlowUpdateProcessor &operator=(const SlowUpdateProcessor &other) = delete;

public:
    bool init(const DocProcessorInitParam &param) override;
    bool process(const document::ExtendDocumentPtr &document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs) override;
    void destroy() override;
    SlowUpdateProcessor *clone() override;
    ExecuteProcessor *allocate() override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

public:
    Config getConfig() const {
        if (_configPath.empty()) {
            return _config;
        }
        autil::ScopedReadLock l(_rwLock);
        return _config;
    }

private:
    void syncConfig();

public:
    static ActionType str2Action(const std::string& str);
    static std::string action2Str(ActionType action);

private:
    mutable autil::ReadWriteLock _rwLock;
    Config _config;
    std::string _configPath;
    autil::LoopThreadPtr _syncThread;
    bool _autoSync;

private:
    BS_LOG_DECLARE();
    friend class SlowUpdateProcessorTest;
    friend class SlowUpdateProcessor::ExecuteProcessor;    
};

BS_TYPEDEF_PTR(SlowUpdateProcessor);

}
}

#endif //ISEARCH_BS_SLOWUPDATEPROCESSOR_H
