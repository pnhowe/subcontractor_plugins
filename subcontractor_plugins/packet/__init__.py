MODULE_NAME = 'packet'

from subcontractor_plugins.packet.lib import create, destroy, set_power, power_state, device_state, get_interface_map

MODULE_FUNCTIONS = {
                     'create': create,
                     'destroy': destroy,
                     'set_power': set_power,
                     'power_state': power_state,
                     'device_state': device_state,
                     'get_interface_map': get_interface_map
                   }
