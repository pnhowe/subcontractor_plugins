MODULE_NAME = 'libvirt'

from subcontractor_plugins.libvirt.lib import create, create_rollback, destroy, set_power, power_state, get_interface_map

MODULE_FUNCTIONS = {
                     'create': create,
                     'create_rollback': create_rollback,
                     'destroy': destroy,
                     'set_power': set_power,
                     'power_state': power_state,
                     'get_interface_map': get_interface_map
                   }
