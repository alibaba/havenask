package(default_visibility=['//aios/network/gig:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_library(
    name='multi_call',
    deps=[
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/network/gig/multi_call/service:service_public_headers',
        '//aios/network/gig/multi_call/rpc:rpc_public_headers',
        '//aios/network/gig/multi_call/arpc:arpc_public_headers',
        '//aios/network/gig/multi_call/agent:agent_public_headers',
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/subscribe:subscribe_headers',
        '//aios/network/gig/multi_call/metric:metric_headers',
        ':common_proto_cc_proto_headers', ':jni_proto_cc_proto_headers',
        '@grpc//:grpc++', '//aios/network/gig/multi_call/interface:interface',
        '//aios/network/gig/multi_call/service:service',
        '//aios/network/gig/multi_call/stream:stream',
        '//aios/network/gig/multi_call/subscribe:subscribe',
        '//aios/network/gig/multi_call/metric:metric',
        '//aios/network/gig/multi_call/agent:agent',
        '//aios/network/gig/multi_call/rpc:rpc',
        '//aios/network/gig/multi_call/new_heartbeat:new_heartbeat',
        '//aios/network/gig/multi_call/java:java'
    ],
    copts=['-Werror'],
    visibility=['//visibility:public'],
    alwayslink=1
)
filter_static_Libs = '`echo -e "srcs=\'%s\'.split()\\nsrcs=[x for x in srcs if not x.endswith(\'.so\')]\\npic_srcs=[x for x in srcs if x.endswith(\'pic.a\') or x.endswith(\'pic.lo\')]\\nsrcs=pic_srcs if pic_srcs else [x for x in srcs if x not in pic_srcs]\\nprint(\' \'.join(srcs))" | python3`'
whole_libs = (
    filter_static_Libs %
    '$(locations //third_party/glibc-compatibility:__cxa_thread_atexit_impl) $(locations //third_party/glibc-compatibility:__wrap_pthread_sigmask) $(locations //aios/network/gig/multi_call/interface:interface) $(locations //aios/network/gig/multi_call/service:service) $(locations //aios/network/gig/multi_call/stream:stream) $(locations //aios/network/gig/multi_call/subscribe:subscribe) $(locations //aios/network/gig/multi_call/metric:metric) $(locations //aios/network/gig/multi_call/agent:agent) $(locations //aios/network/gig/multi_call/rpc:rpc) $(locations //aios/network/gig/multi_call/new_heartbeat:new_heartbeat) $(locations //aios/network/gig/multi_call/java:java) $(locations //aios/network/gig/multi_call/common:common) $(locations //aios/network/gig/multi_call/config:config) $(locations //aios/network/gig/multi_call/util:util) $(locations //aios/network/gig/multi_call/grpc:grpc) $(locations //aios/network/gig/multi_call/controller:controller) $(locations //aios/network/gig/multi_call/java:jni) $(locations //aios/network/gig/multi_call/arpc:arpc) $(locations :heartbeat_proto_cc_impl) $(locations :new_heartbeat_proto_cc_impl) $(locations :agent_proto_cc_impl) $(locations :jni_proto_cc_impl) $(locations :stream_proto_cc_impl) $(locations :common_proto_cc_impl) $(locations //aios/network/arpc:proto_cc_impl) $(locations //aios/apps/facility/cm2/cm_basic/basic_struct:basic_struct) $(locations //aios/apps/facility/cm2/cm_basic/util:util) $(locations //aios/apps/facility/cm2/cm_basic/config:config) $(locations //aios/apps/facility/cm2/cm_sub:cm_sub) $(locations //aios/apps/facility/cm2/cm_basic/leader_election:leader_election) $(locations //aios/apps/facility/cm2/cm_sub/config:config) $(locations //aios/apps/facility/cm2/cm_sub/conhash:conhash) $(locations //aios/apps/facility/cm2/cm_basic/basic_struct:xds_proto_cc_impl) $(locations //aios/filesystem/fslib/fslib/cache:cache) $(locations //aios/filesystem/fslib/fslib/util:util) $(locations //aios/filesystem/fslib/fslib/fs/local:local) $(locations //aios/filesystem/fslib/fslib/fs:fs) $(locations //third_party/easy:easy_static) $(locations //third_party/glibc-compatibility:glibc-compatibility) $(locations @grpc//:grpc_base_c) $(locations @grpc//:alts_util) $(locations @grpc//:grpc_secure) $(locations @grpc//:grpc++) $(locations @grpc//:gpr_base) $(locations @grpc//:grpc) $(locations @grpc//:grpc_cfstream) $(locations @grpc//:grpc_base) $(locations @grpc//:census) $(locations @grpc//:grpc_deadline_filter) $(locations @grpc//:grpc_client_authority_filter) $(locations @grpc//:grpc_lb_policy_pick_first) $(locations @grpc//:grpc_client_channel) $(locations @grpc//:health_proto) $(locations @grpc//:grpc_trace) $(locations @grpc//:grpc_lb_policy_round_robin) $(locations @grpc//:grpc_max_age_filter) $(locations @grpc//:grpc_message_size_filter) $(locations @grpc//:grpc_resolver_dns_ares) $(locations @grpc//:grpc_resolver_dns_native) $(locations @grpc//:grpc_resolver_sockaddr) $(locations @grpc//:grpc_transport_chttp2_client_insecure) $(locations @grpc//:grpc_transport_chttp2) $(locations @grpc//:grpc_http_filters) $(locations @grpc//:grpc_transport_chttp2_alpn) $(locations @grpc//:grpc_transport_chttp2_client_connector) $(locations @grpc//:grpc_transport_chttp2_server_insecure) $(locations @grpc//:grpc_transport_chttp2_server) $(locations @grpc//:grpc_transport_inproc) $(locations @grpc//:grpc_workaround_cronet_compression_filter) $(locations @grpc//:grpc_server_backward_compatibility) $(locations @grpc//:grpc_lb_policy_grpclb_secure) $(locations @grpc//:alts_proto) $(locations @grpc//:tsi) $(locations @grpc//:alts_frame_protector) $(locations @grpc//:tsi_interface) $(locations @grpc//:grpclb_proto) $(locations @grpc//:grpc_lb_policy_xds_secure) $(locations @grpc//:grpc_transport_chttp2_client_secure) $(locations @grpc//:grpc_transport_chttp2_server_secure) $(locations @grpc//:grpc++_codegen_base_src) $(locations @grpc//:grpc++_base) $(locations //aios/network/gig/grpc_deps_compatible:fake_resolver)'
)
dep_libs = (
    filter_static_Libs %
    '$(locations //aios/network/http_arpc:http_arpc) $(locations //aios/kmonitor:kmonitor_client_cpp) $(locations //aios/kmonitor:kmonitor_cpp_adapter) $(locations //aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_impl) $(locations //aios/network/opentelemetry/core:opentelemetry) $(locations //aios/network/opentelemetry/core:otlp_cc_impl) $(locations //aios/autil:mem_pool_base) $(locations //aios/autil:fast_json) $(locations //aios/autil:json_base) $(locations //aios/autil:exception) $(locations //aios/autil:string_type) $(locations //aios/autil:string_helper) $(locations //aios/autil:bitmap) $(locations //aios/autil:murmur_hash) $(locations //aios/autil:multi_value_helper) $(locations //aios/alog:alog) $(locations //aios/network/arpc:arpc) $(locations //aios/network/anet:anet) $(locations @com_google_protobuf//:protobuf) $(locations @com_google_protobuf//:protobuf_lite) $(locations //aios/autil:time) $(locations //aios/autil:lock) $(locations //aios/autil:net) $(locations //aios/autil:thread) $(locations //aios/autil:env_util) $(locations //aios/autil:regex) $(locations //aios/autil:data_buffer) $(locations //aios/autil:hash_base) $(locations //aios/autil:base64) $(locations //aios/autil:daily_run_mode) $(locations //aios/autil:url_util) $(locations //aios/autil:metric) $(locations //aios/autil:zlib) $(locations //aios/autil:thread_name_scope) $(locations //aios/autil:lock_free) $(locations //aios/autil:cityhash) $(locations //aios/autil:hash_func) $(locations //aios/autil:stack_tracer) $(locations //aios/alog:alog) $(locations //third_party/zookeeper:zookeeper_static) $(locations //third_party/mxml:mxml_static) $(locations @vipserver) $(locations @zlib_archive//:zlib) $(locations @curl) $(locations @snappy) $(locations @com_github_nanopb_nanopb//:nanopb) $(locations @jsoncpp_git//:jsoncpp) $(locations //aios/memory/lockless_allocator:ll_alloc_export) $(locations //aios/network/arpc/arpc/metric:arpc_metric) $(locations @boost//:system) $(locations @boringssl//:ssl) $(locations @boringssl//:crypto)'
)
wrap_func_link_opt = '-Wl,--wrap=pthread_sigmask'
genrule(
    name='libjgig_glibc_2_17',
    srcs=[
        '//aios/network/gig/multi_call/interface:interface',
        '//aios/network/gig/multi_call/service:service',
        '//aios/network/gig/multi_call/stream:stream',
        '//aios/network/gig/multi_call/subscribe:subscribe',
        '//aios/network/gig/multi_call/metric:metric',
        '//aios/network/gig/multi_call/agent:agent',
        '//aios/network/gig/multi_call/rpc:rpc',
        '//aios/network/gig/multi_call/new_heartbeat:new_heartbeat',
        '//aios/network/gig/multi_call/java:java',
        '//aios/network/gig/multi_call/common:common',
        '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig/multi_call/util:util',
        '//aios/network/gig/multi_call/controller:controller',
        '//aios/network/gig/multi_call/grpc:grpc',
        '//aios/network/gig/multi_call/arpc:arpc',
        '//aios/network/gig/multi_call/java:jni',
        ':new_heartbeat_proto_cc_impl', ':heartbeat_proto_cc_impl',
        ':agent_proto_cc_impl', ':jni_proto_cc_impl', ':stream_proto_cc_impl',
        ':common_proto_cc_impl', '@grpc//:grpc++', '@grpc//:gpr_base',
        '@grpc//:grpc', '@grpc//:grpc_cfstream', '@grpc//:grpc_base',
        '@grpc//:grpc_base_c', '@grpc//:census', '@grpc//:grpc_deadline_filter',
        '@grpc//:grpc_client_authority_filter',
        '@grpc//:grpc_lb_policy_pick_first', '@grpc//:grpc_client_channel',
        '@grpc//:health_proto', '@grpc//:grpc_trace',
        '@grpc//:grpc_lb_policy_round_robin', '@grpc//:grpc_max_age_filter',
        '@grpc//:grpc_message_size_filter', '@grpc//:grpc_resolver_dns_ares',
        '@grpc//:grpc_resolver_dns_native', '@grpc//:grpc_resolver_sockaddr',
        '@grpc//:grpc_transport_chttp2_client_insecure',
        '@grpc//:grpc_transport_chttp2', '@grpc//:grpc_http_filters',
        '@grpc//:grpc_transport_chttp2_alpn',
        '@grpc//:grpc_transport_chttp2_client_connector',
        '@grpc//:grpc_transport_chttp2_server_insecure',
        '@grpc//:grpc_transport_chttp2_server', '@grpc//:grpc_transport_inproc',
        '@grpc//:grpc_workaround_cronet_compression_filter',
        '@grpc//:grpc_server_backward_compatibility',
        '@grpc//:grpc_lb_policy_grpclb_secure', '@grpc//:grpc_secure',
        '@grpc//:alts_util', '@grpc//:alts_proto', '@grpc//:tsi',
        '@grpc//:alts_frame_protector', '@grpc//:tsi_interface',
        '@grpc//:grpclb_proto', '@grpc//:grpc_lb_policy_xds_secure',
        '@grpc//:grpc_transport_chttp2_client_secure',
        '@grpc//:grpc_transport_chttp2_server_secure',
        '@grpc//:grpc++_codegen_base_src', '@grpc//:grpc++_base',
        '//aios/network/gig/grpc_deps_compatible:fake_resolver',
        '//aios/network/arpc:arpc', '//aios/network/arpc:proto_cc_impl',
        '//aios/network/arpc/arpc/metric:arpc_metric',
        '//aios/network/anet:anet', '//aios/network/http_arpc:http_arpc',
        '//aios/network/opentelemetry/core:opentelemetry',
        '//aios/network/opentelemetry/core:otlp_cc_impl',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_impl',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:xds_proto_cc_impl',
        '//aios/apps/facility/cm2/cm_basic/util:util',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:basic_struct',
        '//aios/apps/facility/cm2/cm_basic/config:config',
        '//aios/apps/facility/cm2/cm_basic/leader_election:leader_election',
        '//aios/apps/facility/cm2/cm_sub:cm_sub',
        '//aios/apps/facility/cm2/cm_sub/config:config',
        '//aios/apps/facility/cm2/cm_sub/conhash:conhash',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/kmonitor:kmonitor_cpp_adapter',
        '//aios/filesystem/fslib/fslib/util:util',
        '//aios/filesystem/fslib/fslib/cache:cache',
        '//aios/filesystem/fslib/fslib/fs/local:local',
        '//aios/filesystem/fslib/fslib/fs:fs', '//aios/autil:exception',
        '//aios/autil:mem_pool_base', '//aios/autil:string_type',
        '//aios/autil:string_helper', '//aios/autil:bitmap',
        '//aios/autil:fast_json', '//aios/autil:json_base', '//aios/autil:json',
        '//aios/autil:murmur_hash', '//aios/autil:multi_value_helper',
        '//aios/autil:time', '//aios/autil:lock', '//aios/autil:net',
        '//aios/autil:thread', '//aios/autil:env_util', '//aios/autil:regex',
        '//aios/autil:data_buffer', '//aios/autil:hash_base',
        '//aios/autil:base64', '//aios/autil:daily_run_mode',
        '//aios/autil:zlib', '//aios/autil:url_util', '//aios/autil:metric',
        '//aios/autil:thread_name_scope', '//aios/autil:lock_free',
        '//aios/autil:cityhash', '//aios/autil:stack_tracer',
        '//aios/autil:hash_func', '//aios/alog:alog',
        '//aios/memory/lockless_allocator:ll_alloc_export',
        '//third_party/zookeeper:zookeeper_static',
        '//third_party/mxml:mxml_static', '//third_party/easy:easy_static',
        '//third_party/glibc-compatibility:glibc-compatibility',
        '//third_party/glibc-compatibility:__cxa_thread_atexit_impl',
        '//third_party/glibc-compatibility:__wrap_pthread_sigmask', '@snappy',
        '@vipserver', '@curl', '@zlib_archive//:zlib', '@boost//:system',
        '@jsoncpp_git//:jsoncpp', '@boringssl//:crypto', '@boringssl//:ssl',
        '@com_google_protobuf//:protobuf',
        '@com_google_protobuf//:protobuf_lite',
        '@com_github_nanopb_nanopb//:nanopb'
    ],
    outs=['libjgig_glibc_2_17.so'],
    cmd=((((((
        'g++ -m64 -shared -static-libstdc++ -lpthread -ldl ' + wrap_func_link_opt
    ) + ' -Wl,--whole-archive ') + whole_libs) + ' -Wl,--no-whole-archive ') +
          dep_libs) + ' -o $(OUTS)'),
    visibility=['//visibility:public']
)
cc_proto(
    name='heartbeat_proto',
    srcs=['multi_call/proto/Heartbeat.proto'],
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='new_heartbeat_proto',
    srcs=['multi_call/proto/NewHeartbeat.proto'],
    deps=[':agent_proto']
)
cc_proto(
    name='agent_proto',
    srcs=['multi_call/proto/GigAgent.proto'],
    deps=[':common_proto']
)
cc_proto(
    name='jni_proto',
    srcs=[
        'multi_call/proto/GigSelectProto.proto',
        'multi_call/proto/GigCallProto.proto'
    ],
    deps=[':common_proto']
)
cc_proto(
    name='stream_proto',
    srcs=['multi_call/proto/GigStreamHeader.proto'],
    deps=[]
)
cc_proto(
    name='common_proto',
    srcs=[
        'multi_call/proto/GigErrorCodeProto.proto',
        'multi_call/proto/GigCommonProto.proto'
    ],
    deps=[]
)
