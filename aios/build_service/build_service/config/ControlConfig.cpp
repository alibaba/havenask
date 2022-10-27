#include "build_service/config/ResourceReader.h"
#include "build_service/config/ControlConfig.h"

using namespace std;

namespace build_service {
namespace config {

void ControlConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("is_inc_processor_exist", isIncProcessorExist, isIncProcessorExist);
}

}
}
