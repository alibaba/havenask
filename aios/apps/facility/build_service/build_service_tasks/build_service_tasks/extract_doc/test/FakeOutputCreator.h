#pragma once
#include <unordered_map>

#include "build_service/common_define.h"
#include "build_service/io/FileOutput.h"
#include "build_service/io/MultiFileOutput.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/extract_doc/RawDocumentOutput.h"
#include "build_service_tasks/extract_doc/test/FakeSwiftOutput.h"

namespace build_service { namespace task_base {

class FakeOutputCreator : public io::OutputCreator
{
public:
    bool init(const config::TaskOutputConfig& outputConfig) override
    {
        _taskOutputConfig = outputConfig;
        return true;
    }

    io::OutputPtr create(const KeyValueMap& params) override
    {
        _output.reset(doCreate(_taskOutputConfig));
        if (_output->init(params)) {
            return _output;
        }
        return io::OutputPtr();
    }

    io::OutputPtr getOutput() { return _output; }

private:
    virtual io::Output* doCreate(const config::TaskOutputConfig&) = 0;

private:
    config::TaskOutputConfig _taskOutputConfig;
    io::OutputPtr _output;
};
BS_TYPEDEF_PTR(FakeOutputCreator);

class FakeSwiftOutputCreator : public FakeOutputCreator
{
private:
    io::Output* doCreate(const config::TaskOutputConfig& config) override { return new FakeSwiftOutput(config); }
};
BS_TYPEDEF_PTR(FakeSwiftOutputCreator);

class FakeTaskFactory : public TaskFactory
{
public:
    TaskPtr createTask(const std::string& taskName) override { return TaskPtr(); }
    io::InputCreatorPtr getInputCreator(const config::TaskInputConfig& inputConfig) override
    {
        return io::InputCreatorPtr();
    }
    io::OutputCreatorPtr getOutputCreator(const config::TaskOutputConfig& outputConfig) override
    {
        io::OutputCreatorPtr creator;
        if (outputConfig.getType() == io::SWIFT) {
            creator = io::OutputCreatorPtr(new FakeSwiftOutputCreator());
        } else if (outputConfig.getType() == io::FILE) {
            creator = io::OutputCreatorPtr(new io::FileOutputCreator());
        } else if (outputConfig.getType() == io::MULTI_FILE) {
            creator = io::OutputCreatorPtr(new io::MultiFileOutputCreator());
        } else {
            return io::OutputCreatorPtr();
        }

        if (!creator->init(outputConfig)) {
            return io::OutputCreatorPtr();
        }
        return creator;
    }
};

class FakeRawDocumentOutputCreator : public FakeOutputCreator
{
private:
    io::Output* doCreate(const config::TaskOutputConfig& config) override
    {
        build_service_tasks::RawDocumentOutput* ret = new build_service_tasks::RawDocumentOutput(config);
        ret->TEST_setFactory(TaskFactoryPtr(new FakeTaskFactory));
        return ret;
    }
};
BS_TYPEDEF_PTR(FakeRawDocumentOutputCreator);

}} // namespace build_service::task_base
