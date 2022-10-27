#include "indexlib/plugin/test/NamedFactoryModule.h"
#include <cassert>

IE_NAMESPACE_BEGIN(plugin);

class NamedFactoryModuleImpl : public NamedFactoryModule
{
public:
    NamedFactoryModuleImpl(const std::string &suffix);
    ~NamedFactoryModuleImpl();
private:
    NamedFactoryModuleImpl(const NamedFactoryModuleImpl &);
    NamedFactoryModuleImpl& operator=(const NamedFactoryModuleImpl &);
public:
    bool init() override {
        return true;
    }
    void destroy() override {
        delete this;
    }
    const std::string &getSuffix() const override {
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
void destroyFactory(ModuleFactory *factory) {
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
void destroyFactory_TestSuffix(ModuleFactory *factory) {
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
void destroyFactory_OtherTest(ModuleFactory *factory) {
    NamedFactoryModuleImpl *f = dynamic_cast<NamedFactoryModuleImpl*>(factory);
    assert(f->getSuffix() == "TestSuffix2");
    (void)f;
    factory->destroy();
}

IE_NAMESPACE_END(plugin);

