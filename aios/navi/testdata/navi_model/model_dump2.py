import tensorflow as tf
import sys
import os
import pdb
import shutil
import numpy as np

class NaviModelTest(object):
    def __init__(self, root_dir, model_builder, x_train, y_train, x_test, y_test, epochs=1):
        super(NaviModelTest, self).__init__()
        self.root_dir = root_dir
        self.graph = tf.Graph()
        self.session = tf.Session(graph=self.graph)
        self.model = None
        self.model_input = None
        self.model_output = None
        self.graph_path = os.path.join(self.root_dir, "graph")
        self.test_input_tensor_file = os.path.join(self.root_dir, "input_tensor")
        self.test_result_tensor_file = os.path.join(self.root_dir, "result_tensor")
        self.tensorboard_log_dir = os.path.join(root_dir, "tensorboard_log_dir")
        self.tensorboard_callback = tf.keras.callbacks.TensorBoard(
            log_dir=self.tensorboard_log_dir)
        self.x_train = x_train
        self.y_train = y_train
        self.x_test = x_test
        self.y_test = y_test
        self.epochs = epochs
        self.test_output = None
        self.init_model(model_builder)

    def init_model(self, model_builder):
        with self.graph.as_default():
            self.model = model_builder()
            assert len(self.model.inputs) == 1, 'model inputs len must be 1, actual {}'.format(len(self.model.inputs))
            assert len(self.model.outputs) == 1, 'model outputs len must be 1, actual {}'.format(len(self.model.outputs))
            self.model_input = tf.placeholder(self.model.inputs[0].dtype,
                                              shape=self.model.inputs[0].shape,
                                              name='model_input')
            self.model_output = tf.identity(self.model(self.model_input),
                                            name='model_output')

    def run(self):
        with self.graph.as_default():
            with self.session.as_default():
                self.model.fit(self.x_train, self.y_train, epochs = self.epochs,
                               callbacks = [self.tensorboard_callback])
                loss, acc = self.model.evaluate(self.x_test, self.y_test, verbose=2)
                self.test_output = self.model.predict(self.x_test)

    def dump(self):
        shutil.rmtree(self.root_dir, True)
        os.makedirs(self.root_dir)
        output_node_name = self.model_output.op.name
        output_graph_def = tf.graph_util.convert_variables_to_constants(
            self.session, self.graph.as_graph_def(), [output_node_name])
        find = False
        for node in output_graph_def.node:
            if not find and node.op == 'MatMul':
                node.device = '/GPU:0'
                find = True
            else:
                node.device = '/CPU:0'

        with open('{}.pb'.format(self.graph_path), 'wb') as fp:
            fp.write(output_graph_def.SerializeToString())
        tf.train.write_graph(output_graph_def, '.', '{}.pbtxt'.format(self.graph_path))

        for node in output_graph_def.node:
            node.device = '/CPU:0'

        with open('{}_cpu.pb'.format(self.graph_path), 'wb') as fp:
            fp.write(output_graph_def.SerializeToString())
        tf.train.write_graph(output_graph_def, '.', '{}_cpu.pbtxt'.format(self.graph_path))

        with tf.Graph().as_default() as g:
            with tf.Session() as sess:
                input_content = sess.run(tf.serialize_tensor(self.x_test.astype(np.float32)))
                result_content = sess.run(tf.serialize_tensor(self.test_output.astype(np.float32)))
                open(self.test_input_tensor_file, 'wb').write(input_content)
                open(self.test_result_tensor_file, 'wb').write(result_content)

def create_conv_model():
    model = tf.keras.models.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(28, 28, 1)),
            tf.keras.layers.Conv2D(32, kernel_size=(3, 3), activation="relu"),
            tf.keras.layers.MaxPooling2D(pool_size=(2, 2)),
            tf.keras.layers.Conv2D(64, kernel_size=(3, 3), activation="relu"),
            tf.keras.layers.MaxPooling2D(pool_size=(2, 2)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dropout(0.5),
            tf.keras.layers.Dense(10, activation=tf.nn.softmax),
        ]
    )
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])
    return model

def create_simple_model():
    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(512, activation=tf.nn.relu),
        tf.keras.layers.Dense(512, activation=tf.nn.relu),
        tf.keras.layers.Dense(512, activation=tf.nn.relu),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(10, activation=tf.nn.softmax)
    ])
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])
    return model

mnist = tf.keras.datasets.mnist
(x_train, y_train),(x_test, y_test) = mnist.load_data(path="/home/shiautsung.wxc/tf_python/mnist.npz")
x_train, x_test = x_train / 255.0, x_test / 255.0

# case 1
navi_simple_model = NaviModelTest("./navi_simple_model", create_simple_model,
                                  x_train, y_train, x_test, y_test)
navi_simple_model.run()
navi_simple_model.dump()
# case 2
# x_train = np.reshape(x_train, (-1,28,28,1))
# x_test = np.reshape(x_test, (-1,28,28,1))
# navi_conv_model = NaviModelTest("./navi_model", create_conv_model,
#                                 x_train, y_train, x_test, y_test)
# navi_conv_model.run()
# navi_conv_model.dump()
