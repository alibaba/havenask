#ifndef ISEARCH_BS_FAKEBUILDER_H
#define ISEARCH_BS_FAKEBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"

namespace build_service {
namespace workflow {

class FakeBuilder : public builder::Builder
{
public:
    FakeBuilder();
    ~FakeBuilder();
private:
    FakeBuilder(const FakeBuilder &);
    FakeBuilder& operator=(const FakeBuilder &);
    
public:
    /*override*/ bool build(const IE_NAMESPACE(document)::DocumentPtr &doc);
    /*override*/ int64_t getIncVersionTimestamp() const
    { 
        return 0;
    }
    
    /*override*/ RET_STAT getIncVersionTimestampNonBlocking(int64_t& ts) const
    { 
        ts = 0;
        return RS_OK;
    }

    /*override*/ bool getLastLocator(common::Locator &locator) const
    { 
        locator = _lastLocator;
        return true;
    }

    /*override*/ RET_STAT getLastLocatorNonBlocking(common::Locator &locator) const
    { 
        locator = _lastLocator;
        return RS_OK;
    }

    /*override*/ void stop(int64_t stopTimestamp = INVALID_TIMESTAMP) {}

    uint32_t getBuildCount() const { return _buildCount; }

private:
    common::Locator _lastLocator;
    uint32_t _buildCount;
    
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_FAKEBUILDER_H
