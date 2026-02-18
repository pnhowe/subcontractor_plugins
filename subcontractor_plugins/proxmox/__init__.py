MODULE_NAME = 'proxmox'

from subcontractor_plugins.proxmox.lib import create, create_rollback, destroy, get_interface_map, set_power, power_state

MODULE_FUNCTIONS = {
                     'create': create,
                     'create_rollback': create_rollback,
                     'destroy': destroy,
                     'get_interface_map': get_interface_map,
                     'set_power': set_power,
                     'power_state': power_state,
                   }
