 # Python
from __future__ import print_function
import tensorflow as tf
from tensorflow.python.ops import control_flow_ops
ways = [1, 2, 4, 8, 16, 32]

for way_count in ways:
    g = tf.Graph()
    batch_size = 1024
    with g.as_default():
        turing_agg_module = tf.load_op_library('/ha3_develop/source_code/ha3/build/release64/lib/libha3_suez_turing_agg_opdef.so')
        input_request = tf.placeholder(tf.variant, name='ha3_request')
        request = turing_agg_module.request_init_op(input_request, name = 'init_request')
        layer_metas = turing_agg_module.layer_metas_create_op(request, name = 'create_range')
        aggres = []
        split_layer_metas = turing_agg_module.range_split_op(layer_metas, N = way_count,
                                                             name = 'split_range')
        for layer_metas in split_layer_metas:
            expression_resource = turing_agg_module.prepare_expression_resource_op(request,
                                                                                   name = "resource_prepare")
            seek_iterator = turing_agg_module.seek_iterator_prepare_op(layer_metas, expression_resource, name = "seek_prepare")
            with tf.control_dependencies([seek_iterator]):
                aggregator = turing_agg_module.agg_prepare_op(expression_resource, name = "agg_prepare")
            agg = turing_agg_module.seek_and_agg_op(aggregator, seek_iterator,
                                                    batch_size = batch_size, name = "seek_and_agg")
            aggres.append(agg)

        aggresult = turing_agg_module.agg_merge_op(aggres)
        ha3result = turing_agg_module.agg_result_construct_op(aggresult, name = "ha3_result")
        with tf.control_dependencies([ha3result]):
            node_end = tf.no_op(name="ha3_search_done")

        g_def = g.as_graph_def()

        with open("./default_agg_%d.def" % way_count, 'w') as f:
            f.write(g_def.SerializeToString())
            with open("./default_agg_%d.pbtxt" % way_count, 'w') as f:
                f.write(str(g_def))
