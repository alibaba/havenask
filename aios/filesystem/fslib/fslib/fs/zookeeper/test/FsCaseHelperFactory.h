#ifndef FSLIB_PLUGIN_FSCASEHELPERFACTORY_H
#define FSLIB_PLUGIN_FSCASEHELPERFACTORY_H

#include "fslib/common.h"
#include "fslib/fs/zookeeper/test/FsCaseHelperBase.h"
#include "fslib/util/Singleton.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class FsCaseHelperFactory : public fslib::util::Singleton<FsCaseHelperFactory> {
public:
    friend class fslib::util::LazyInstantiation;

public:
    typedef std::map<FsType, FsCaseHelperBase *> FsCaseHelperMap;

public:
    ~FsCaseHelperFactory();

private:
    FsCaseHelperFactory();

public:
    FsCaseHelperBase *getFsCaseHelper(const FsType type);

private:
    FsCaseHelperMap _fsCaseHelperMap;
};

typedef std::unique_ptr<FsCaseHelperFactory> FsCaseHelperFactoryPtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_FSCASEHELPERFACTORY_H
