#include "build_service/task_base/test/FakeAlterFieldMergerFactory.h"
#include "build_service/task_base/test/FakeAlterFieldMerger.h"

using namespace std;
using namespace build_service::custom_merger;

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, FakeAlterFieldMergerFactory);

FakeAlterFieldMergerFactory::FakeAlterFieldMergerFactory() {
}

FakeAlterFieldMergerFactory::~FakeAlterFieldMergerFactory() {
}

CustomMerger* FakeAlterFieldMergerFactory::createCustomMerger(const std::string &mergerName) {
    return new FakeAlterFieldMerger();
}

}
}

extern "C"
build_service::plugin::ModuleFactory* createFactory_Merger() {
    return new build_service::task_base::FakeAlterFieldMergerFactory;
}

extern "C"
void destroyFactory_Merger(build_service::plugin::ModuleFactory* factory) {
    factory->destroy();
}
