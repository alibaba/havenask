#include "build_service/plugin/test/NamedFactoryModule.h"

namespace build_service {
namespace plugin {

class NamedFactoryModuleImpl : public NamedFactoryModule
{
public:
    NamedFactoryModuleImpl(const std::string &suffix);
    ~NamedFactoryModuleImpl();
private:
    NamedFactoryModuleImpl(const NamedFactoryModuleImpl &);
    NamedFactoryModuleImpl& operator=(const NamedFactoryModuleImpl &);
public:
    /* override */ bool init(const KeyValueMap &parameters) {
        return true;
    }
    /* override */ void destroy() {
        delete this;
    }
    const std::string &getSuffix() const {
        return _suffix;
    }
private:
    std::string _suffix;
};


NamedFactoryModuleImpl::NamedFactoryModuleImpl(const std::string &suffix) {
    _suffix = suffix;
}

NamedFactoryModuleImpl::~NamedFactoryModuleImpl() {
}

extern "C"
ModuleFactory* createFactory() {
    return new NamedFactoryModuleImpl("Normal");
}

extern "C"
void destroyFactory(IE_NAMESPACE(plugin)::ModuleFactory *factory) {
    NamedFactoryModuleImpl *f = dynamic_cast<NamedFactoryModuleImpl*>(factory);
    assert(f->getSuffix() == "Normal");
    (void)f;
    factory->destroy();
}

extern "C"
ModuleFactory* createFactory_TestSuffix() {
    return new NamedFactoryModuleImpl("TestSuffix");
}

extern "C"
void destroyFactory_TestSuffix(IE_NAMESPACE(plugin)::ModuleFactory *factory) {
    NamedFactoryModuleImpl *f = dynamic_cast<NamedFactoryModuleImpl*>(factory);
    assert(f->getSuffix() == "TestSuffix");
    (void)f;
    factory->destroy();
}

extern "C"
ModuleFactory* createFactory_OtherTest() {
    return new NamedFactoryModuleImpl("TestSuffix2");
}

extern "C"
void destroyFactory_OtherTest(IE_NAMESPACE(plugin)::ModuleFactory *factory) {
    NamedFactoryModuleImpl *f = dynamic_cast<NamedFactoryModuleImpl*>(factory);
    assert(f->getSuffix() == "TestSuffix2");
    (void)f;
    factory->destroy();
}

}
}
