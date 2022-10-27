#ifndef ISEARCH_BS_RAWTEXTBUILDER_H
#define ISEARCH_BS_RAWTEXTBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include <indexlib/file_system/buffered_file_writer.h>
#include <indexlib/util/counter/counter_map.h>

namespace build_service {
namespace builder {

class LineDataBuilder : public Builder
{
public:
    LineDataBuilder(const std::string &indexPath,
                   const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
                   fslib::fs::FileLock *fileLock = NULL);
    ~LineDataBuilder();
private:
    LineDataBuilder(const LineDataBuilder &);
    LineDataBuilder& operator=(const LineDataBuilder &);
public:
    bool init(const config::BuilderConfig &builderConfig,
              IE_NAMESPACE(misc)::MetricProviderPtr metricProvider = 
              IE_NAMESPACE(misc)::MetricProviderPtr()) override;
    bool build(const IE_NAMESPACE(document)::DocumentPtr &doc) override;
    void stop(int64_t stopTimestamp = INVALID_TIMESTAMP) override;
    bool merge(const IE_NAMESPACE(config)::IndexPartitionOptions &options) override {
        return false;
    }
    int64_t getIncVersionTimestamp() const override  {
        return -1;
    }
    bool getLastLocator(common::Locator &locator) const override  {
        locator = common::Locator();
        return true;
    }
    bool getLatestVersionLocator(common::Locator &locator) const override  {
        return false;
    }
    const IE_NAMESPACE(util)::CounterMapPtr& GetCounterMap() override
    {
        return _counterMap;
    }
    virtual RET_STAT getIncVersionTimestampNonBlocking(int64_t& ts) const {
        ts = -1;
        return RS_OK;
    }
    
    virtual RET_STAT getLastLocatorNonBlocking(common::Locator &locator) const {
        locator = common::Locator();
        return RS_OK;
    }
public:
    // for test
    void setFileWriter(const IE_NAMESPACE(file_system)::BufferedFileWriterPtr& writer) { _writer = writer; }
public:
    static bool storeSchema(const std::string &indexPath,
                            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema);
private:
    bool prepareOne(const IE_NAMESPACE(document)::DocumentPtr &doc,
                    std::string &oneLineDoc);
private:
    std::string _indexPath;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    IE_NAMESPACE(file_system)::BufferedFileWriterPtr _writer;
    std::string _fieldSeperator;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LineDataBuilder);

}
}

#endif //ISEARCH_BS_RAWTEXTBUILDER_H
