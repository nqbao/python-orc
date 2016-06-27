from py4j.java_gateway import JavaGateway, get_field

__orc_gateway = None


def get_gateway():
    global __orc_gateway

    if __orc_gateway is None:
        __orc_gateway = JavaGateway()

    return __orc_gateway
