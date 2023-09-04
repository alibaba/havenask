#include "fslib/fs/zookeeper/test/FsCaseHelperFactory.h"

#include "fslib/fs/zookeeper/test/ZookeeperFsCaseHelper.h"

using namespace std;
using namespace fslib;
FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
// AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, FsCaseHelperFactory);

FsCaseHelperFactory::FsCaseHelperFactory() {
    _fsCaseHelperMap.insert(pair<FsType, FsCaseHelperBase *>("zfs", new ZookeeperFsCaseHelper()));
}

FsCaseHelperFactory::~FsCaseHelperFactory() {
    FsCaseHelperMap::iterator it = _fsCaseHelperMap.begin();
    while (it != _fsCaseHelperMap.end()) {
        delete it->second;
        it++;
    }
}

FsCaseHelperBase *FsCaseHelperFactory::getFsCaseHelper(const FsType type) {
    FsCaseHelperMap::iterator it = _fsCaseHelperMap.find(type);
    if (it != _fsCaseHelperMap.end()) {
        return it->second;
    }
    return NULL;
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
