from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters, CallbackServerParameters, get_field
import sys, os, glob


def find_jar_path():
    # assume the jar is bundled with python egg
    current_dir = os.path.dirname(os.path.realpath(__file__))
    search = [
        current_dir,
        os.path.join(current_dir, "../../java-gateway/target"),
        os.path.join(current_dir, "../share/python-orc"),
    ]

    for path in search:
        r = glob.glob(os.path.join(path, "gateway*.jar"))
        if len(r):
            return r[0]

    return ""


def get_gateway(extra_jar=None):

    if extra_jar is None:
        extra_jar = find_jar_path()

    port = launch_gateway(
        die_on_exit=True, classpath=extra_jar,
        # redirect_stderr=sys.stderr, redirect_stdout=sys.stdout
    )

    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port),
                          callback_server_parameters=CallbackServerParameters(port=0))

    # retrieve the port on which the python callback server was bound to.
    python_port = gateway.get_callback_server().get_listening_port()

    gateway.java_gateway_server.resetCallbackClient(
        gateway.java_gateway_server.getCallbackClient().getAddress(),
        python_port)

    return gateway
