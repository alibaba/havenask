#include "build_service/config/DocReclaimSource.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, DocReclaimSource);

string DocReclaimSource::DOC_RECLAIM_SOURCE = "doc_reclaim_source";

DocReclaimSource::DocReclaimSource()
    : msgTTLInSeconds(-1)
{
}

DocReclaimSource::~DocReclaimSource() {
}

void DocReclaimSource::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("swift_root", swiftRoot);
    json.Jsonize("topic_name", topicName);
    json.Jsonize("swift_client_config", clientConfigStr, clientConfigStr);
    json.Jsonize("swift_reader_config", readerConfigStr, readerConfigStr);
    json.Jsonize("ttl_in_seconds", msgTTLInSeconds, msgTTLInSeconds);
}

}
}
