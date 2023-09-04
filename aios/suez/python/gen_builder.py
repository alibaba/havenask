import sys
import os


class ProtoBuilderGenerator:
    def __init__(self, pb2_file):
        self.pb2_file = pb2_file
        self.module_name = os.path.splitext(os.path.basename(pb2_file))[0]
        self.import_module = "import " + self.module_name

    def generate_builder_code(self):
        pb2_module = __import__(self.module_name)

        print("from aios.suez.admin.ClusterService_pb2 import ClusterConfig, Cluster, ClusterDeploymentConfig\n\n")
        for message_descriptor in pb2_module.DESCRIPTOR.message_types_by_name.itervalues():
            self.generate_builder_for_message(message_descriptor)

    def generate_builder_for_message(self, message_descriptor):
        class_name = message_descriptor.name + "Builder"
        print("class {}:".format(class_name))

        print("    def __init__(self):")
        print("        self.msg = {}() \n".format(message_descriptor.name))

        for field in message_descriptor.fields:
            print("    def set_{}(self, value):".format(field.name))
            if field.type == field.TYPE_MESSAGE:
                print("        if not isinstance(value, {}):".format(field.message_type.name))
                print(
                    "            raise ValueError('Invalid type for field {}, expected {}')\n".format(
                        field.name, field.message_type.name))

            print("        self.msg.{} = value".format(field.name))
            print("        return self\n")

        print("    def build(self):")
        print("        return self.msg\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: {} example_pb2.py".format(sys.argv[0]))
        sys.exit(1)

    pb2_file = sys.argv[1]
    generator = ProtoBuilderGenerator(pb2_file)
    generator.generate_builder_code()
