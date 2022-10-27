#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/config/TuringOptionsInfo.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

void TuringOptionsInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    _turingOptionsInfo = json.GetMap();
}

END_HA3_NAMESPACE(config);
